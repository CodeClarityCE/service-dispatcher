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

// runReaper loops forever, reconciling stuck analyses once per interval.
func runReaper(db *bun.DB, dr *DependencyResolver, service *boilerplates.ServiceBase) {
	interval := reaperInterval()
	log.Printf("[reaper] started: interval=%s", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		reapOnce(db, dr, service)
	}
}

// reapOnce finds every non-terminal analysis and reconciles each. The finalizer
// takes a row lock, so a reaper pass that races a live completion is safe — one
// wins the lock and transitions, the other no-ops.
//
// The reaper passes allowStageZeroStart=false: it never re-runs js-sbom (stage 0)
// because it cannot guarantee the project source is on disk. It completes
// all-success analyses, fails failed ones, advances stages whose completion was
// lost, and reclaims later-stage steps whose dispatch was lost (see
// stepStuckTimeout). A stuck stage-0 step is failed rather than re-run.
func reapOnce(db *bun.DB, dr *DependencyResolver, service *boilerplates.ServiceBase) {
	ctx := context.Background()

	var candidates []codeclarity.Analysis
	err := db.NewSelect().
		Model(&candidates).
		Column("id").
		Where("status IN (?)", bun.In([]string{
			string(codeclarity.ONGOING),
			string(codeclarity.STARTED),
		})).
		Scan(ctx)
	if err != nil {
		log.Printf("[reaper] candidate query failed: %v", err)
		return
	}
	if len(candidates) == 0 {
		return
	}

	timeout := stepStuckTimeout()
	healed := 0
	for _, c := range candidates {
		if reapAnalysis(c.Id, db, dr, service, timeout) {
			healed++
		}
	}
	log.Printf("[reaper] pass complete: %d candidate(s) examined, %d healed", len(candidates), healed)
}

// reapAnalysis reconciles a single analysis to a fixpoint: it re-runs the
// finalizer until it reports no further change (or reaches a terminal state),
// so several lost-completion stages collapse in one pass instead of one stage
// per tick. Returns true if it made any change. Messages produced along the way
// are published after each committed step.
func reapAnalysis(id uuid.UUID, db *bun.DB, dr *DependencyResolver, service *boilerplates.ServiceBase, timeout time.Duration) bool {
	changed := false
	for i := 0; i < maxReapIterations; i++ {
		msgs, outcome, err := finalizeOrAdvanceStage(id, db, dr, false, timeout)
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
	}
	log.Printf("[reaper] %s did not converge within %d iterations", id, maxReapIterations)
	return changed
}
