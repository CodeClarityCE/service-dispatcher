package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	boilerplates "github.com/CodeClarityCE/utility-boilerplates"
	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// The reaper is a self-healing safety net for the dispatcher. The live message
// handlers finalize analyses on plugin completion, but a completion message can
// be lost (AMQP publish failure, a dropped poison message) or arrive while the
// row is in a transient state. Left alone, such an analysis stays non-terminal
// forever even though all its steps have finished. The reaper periodically
// re-runs the SAME idempotent finalizer over every non-terminal analysis so the
// pipeline always converges.

const (
	defaultReaperInterval = 60 * time.Second
	envReaperInterval     = "REAPER_INTERVAL" // seconds

	// A STARTED step older than this is treated as a lost dispatch and reclaimed.
	// It must be generously larger than any real plugin runtime: a still-running
	// slow plugin whose step is reclaimed would be dispatched a second time
	// concurrently (step writes upsert by name, so last-wins, but it wastes work).
	defaultStepStuckTimeout = 30 * time.Minute
	envStepStuckTimeout     = "REAPER_STEP_TIMEOUT" // seconds

	// Backstop on the per-analysis reconcile loop (heals several lost-completion
	// stages in one pass); far above any real stage count.
	maxReapIterations = 32

	// Non-terminal analyses older than this are retired (marked FAILURE) instead of
	// recovered. No real analysis runs this long, so such rows are abandoned/broken;
	// re-driving them on every restart would re-download thousands of stale repos
	// forever. Must be generously larger than any real end-to-end analysis time.
	defaultRecoveryMaxAge = 24 * time.Hour
	envRecoveryMaxAge     = "RECOVERY_MAX_AGE" // seconds
)

// envDurationSeconds reads an env var as a positive number of seconds, falling
// back to def on unset/invalid values.
func envDurationSeconds(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
		log.Printf("[reaper] invalid %s=%q, using default %s", name, v, def)
	}
	return def
}

// reaperInterval returns the reconcile interval, overridable via REAPER_INTERVAL.
func reaperInterval() time.Duration {
	return envDurationSeconds(envReaperInterval, defaultReaperInterval)
}

// stepStuckTimeout returns the lost-dispatch reclaim threshold, overridable via
// REAPER_STEP_TIMEOUT.
func stepStuckTimeout() time.Duration {
	return envDurationSeconds(envStepStuckTimeout, defaultStepStuckTimeout)
}

// recoveryMaxAge returns the age past which a non-terminal analysis is retired
// rather than recovered, overridable via RECOVERY_MAX_AGE.
func recoveryMaxAge() time.Duration {
	return envDurationSeconds(envRecoveryMaxAge, defaultRecoveryMaxAge)
}

// runReaper recovers orphaned analyses on startup, then loops forever reconciling
// stuck analyses once per interval. The startup pass exists because `make down`
// wipes the (volume-less dev) RabbitMQ queue: every in-flight message is lost
// while the Postgres analysis rows survive, so on boot every non-terminal analysis
// is orphaned and must be re-driven from DB state.
func runReaper(db *bun.DB, dr *DependencyResolver, service *boilerplates.ServiceBase) {
	interval := reaperInterval()
	log.Printf("[reaper] started: interval=%s", interval)

	recoverOnStartup(db, dr, service)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		reapOnce(db, dr, service)
	}
}

// nonTerminalAnalyses returns the ids of every analysis that can be orphaned or
// stuck: the in-flight states (STARTED/ONGOING) plus the API's pre-dispatch states
// (REQUESTED/TRIGGERED). The latter matter because an analysis is created REQUESTED
// and only flips to STARTED once the dispatcher consumes its api_request message —
// if that message is lost (RabbitMQ has no dev volume, so a restart wipes it), the
// analysis is stranded REQUESTED with nothing to drive it. recoverAnalysis treats a
// REQUESTED stage-0 analysis exactly like the live first dispatch (re-drive via the
// downloader, or start stage 0 directly).
//
// Recurring scheduled templates (schedule_type 'daily'/'weekly') also sit in a
// pre-dispatch status indefinitely by design — the scheduler clones them into
// 'once' executions — so they are excluded here to avoid the reaper running a
// template directly. 'once' and legacy NULL rows are eligible.
func nonTerminalAnalyses(ctx context.Context, db *bun.DB) ([]codeclarity.Analysis, error) {
	var candidates []codeclarity.Analysis
	err := db.NewSelect().
		Model(&candidates).
		Column("id").
		Where("status IN (?)", bun.In([]string{
			string(codeclarity.ONGOING),
			string(codeclarity.STARTED),
			string(codeclarity.REQUESTED),
			string(codeclarity.TRIGGERED),
		})).
		Where("(schedule_type IS NULL OR schedule_type = ?)", "once").
		Scan(ctx)
	return candidates, err
}

// recoverOnStartup re-drives every non-terminal analysis once, unconditionally:
// after a restart the queue is empty, so there is nothing in flight to wait for.
// Stage-0/pre-download analyses are re-driven through the downloader; later-stage
// STARTED steps are force-reclaimed (age-independent) and re-dispatched.
func recoverOnStartup(db *bun.DB, dr *DependencyResolver, service *boilerplates.ServiceBase) {
	ctx := context.Background()
	candidates, err := nonTerminalAnalyses(ctx, db)
	if err != nil {
		log.Printf("[reaper] startup candidate query failed: %v", err)
		return
	}
	if len(candidates) == 0 {
		log.Printf("[reaper] startup recovery: no non-terminal analyses")
		return
	}
	timeout := stepStuckTimeout()
	maxAge := recoveryMaxAge()
	recovered := 0
	for _, c := range candidates {
		if recoverAnalysis(c.Id, db, dr, service, timeout, maxAge, true) {
			recovered++
		}
	}
	log.Printf("[reaper] startup recovery complete: %d candidate(s) examined, %d recovered", len(candidates), recovered)
}

// reapOnce finds every non-terminal analysis and reconciles each on the interval.
// The finalizer takes a row lock, so a reaper pass that races a live completion is
// safe — one wins the lock and transitions, the other no-ops.
func reapOnce(db *bun.DB, dr *DependencyResolver, service *boilerplates.ServiceBase) {
	ctx := context.Background()
	candidates, err := nonTerminalAnalyses(ctx, db)
	if err != nil {
		log.Printf("[reaper] candidate query failed: %v", err)
		return
	}
	if len(candidates) == 0 {
		return
	}

	timeout := stepStuckTimeout()
	maxAge := recoveryMaxAge()
	healed := 0
	for _, c := range candidates {
		if recoverAnalysis(c.Id, db, dr, service, timeout, maxAge, false) {
			healed++
		}
	}
	log.Printf("[reaper] pass complete: %d candidate(s) examined, %d healed", len(candidates), healed)
}

// recoverAnalysis reconciles a single non-terminal analysis. It routes stage-0
// recovery (re-run the downloader, then js-sbom) away from the in-place finalizer,
// because stage 0 needs the project source on disk:
//
//   - stage 0 not yet all-success → redriveStageZero when force (startup, queue
//     wiped) or when stuck beyond timeout (interval, avoids racing a fresh submit);
//   - otherwise (stage >= 1, or stage 0 done-but-not-advanced) → the finalizer
//     loop, which advances completed stages and re-dispatches lost later-stage steps.
//
// force re-drives/reclaims regardless of step age (the queue is known-empty at
// startup), but the maxAge cap still applies — an abandoned analysis is retired,
// not endlessly re-downloaded. Returns true if it made any change.
func recoverAnalysis(id uuid.UUID, db *bun.DB, dr *DependencyResolver, service *boilerplates.ServiceBase, timeout, maxAge time.Duration, force bool) bool {
	ctx := context.Background()
	doc := &codeclarity.Analysis{Id: id}
	if err := db.NewSelect().Model(doc).WherePK().Scan(ctx); err != nil {
		log.Printf("[reaper] load %s failed: %v", id, err)
		return false
	}
	switch doc.Status {
	case codeclarity.COMPLETED, codeclarity.FAILURE, codeclarity.CANCELLED, codeclarity.UPDATING_DB:
		return false
	}
	if len(doc.Steps) == 0 {
		return false
	}

	// Retire analyses too old to be worth recovering (abandoned/broken): mark them
	// FAILURE so they stop being candidates, rather than re-downloading stale work.
	if analysisExpired(doc, time.Now(), maxAge) {
		if err := failAnalysis(id, db); err != nil {
			log.Printf("[reaper] retire (fail) expired %s failed: %v", id, err)
			return false
		}
		log.Printf("[reaper] retired expired analysis %s (age cap %s)", id, maxAge)
		return true
	}

	// Stage 0 not yet complete: a stage-0 plugin that reported FAILURE (its
	// completion was lost) must be finalized as failure by the finalizer, not
	// re-run. Otherwise re-drive via the downloader rather than in place, because
	// js-sbom needs the project source on disk.
	if doc.Stage == 0 {
		anyFailure, allSuccess := evaluateStage(doc.Steps[0])
		if !anyFailure && !allSuccess {
			if !force && !stageZeroNeedsRedrive(doc, time.Now(), timeout) {
				return false // fresh submit still downloading — leave it alone
			}
			// A "stuck" stage-0 analysis whose download queue still holds
			// messages is almost certainly just waiting its turn behind a deep
			// backlog — its message is not lost, and re-driving it would add a
			// duplicate to the very queue causing the wait (each interval pass
			// amplifying the backlog further). Re-drive only once the queue has
			// drained; a genuinely lost message is recovered then, merely later.
			// The startup pass (force) skips this: the queue is known-empty.
			if !force {
				if depth, err := service.QueueDepth("dispatcher_downloader"); err == nil && depth > 0 {
					return false
				}
			}
			if err := redriveStageZero(id, db, dr, service); err != nil {
				log.Printf("[reaper] redrive stage-0 %s failed: %v", id, err)
				return false
			}
			log.Printf("[reaper] re-drove stage-0 for %s", id)
			return true
		}
		// stage 0 failed (→ finalize failure) or all-success (→ advance):
		// fall through to the finalizer.
	}

	return reapAnalysis(id, db, dr, service, timeout, force)
}

// reapAnalysis reconciles a single analysis to a fixpoint: it re-runs the
// finalizer until it reports no further change (or reaches a terminal state),
// so several lost-completion stages collapse in one pass instead of one stage
// per tick. Returns true if it made any change. Messages produced along the way
// are published after each committed step.
func reapAnalysis(id uuid.UUID, db *bun.DB, dr *DependencyResolver, service *boilerplates.ServiceBase, timeout time.Duration, force bool) bool {
	changed := false
	for i := 0; i < maxReapIterations; i++ {
		msgs, outcome, err := finalizeOrAdvanceStage(id, db, dr, false, timeout, force)
		if err != nil {
			log.Printf("[reaper] finalize %s failed: %v", id, err)
			return changed
		}
		switch outcome {
		case "":
			return changed
		case "completed", "failure":
			log.Printf("[reaper] finalized %s -> %s", id, outcome)
			return true
		case "dispatched":
			log.Printf("[reaper] re-dispatched %d plugin(s) for %s", len(msgs), id)
			sendMessages(service, msgs)
			changed = true
		case "advanced":
			log.Printf("[reaper] advanced %s", id)
			changed = true
		}
		// force applies only to the first reclaim pass; later iterations advance/
		// dispatch from the reset state without re-reclaiming.
		force = false
	}
	log.Printf("[reaper] %s did not converge within %d iterations", id, maxReapIterations)
	return changed
}
