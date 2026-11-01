package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/CodeClarityCE/utility-boilerplates"
	types_amqp "github.com/CodeClarityCE/utility-types/amqp"
	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uptrace/bun"
)

// pendingMessage holds a message to be published AFTER the DB transaction commits.
// We never publish while holding a row lock — mirror plugin_base.go which commits
// its step update before notifying the dispatcher.
type pendingMessage struct {
	queueName string
	data      []byte
	// plugin + stage identify the dispatched step so the reaper can re-check its
	// status right before publish (see dropCompletedStepMessages).
	plugin string
	stage  int
}

// evaluateStage reports the terminal state of a stage's steps.
//   - anyFailure is true if any step failed.
//   - allSuccess is true if every step succeeded (vacuously true for an empty stage).
func evaluateStage(steps []codeclarity.Step) (anyFailure bool, allSuccess bool) {
	allSuccess = true
	for _, step := range steps {
		if step.Status == codeclarity.FAILURE {
			anyFailure = true
		}
		if step.Status != codeclarity.SUCCESS {
			allSuccess = false
		}
	}
	return anyFailure, allSuccess
}

// reclaimStuckSteps clears later-stage (stage >= 1) STARTED steps whose dispatch
// is assumed lost — those STARTED for longer than reclaimAfter (publish failure or
// a dead plugin), or every STARTED step regardless of age when force is set (a
// restart wiped the queue, so nothing is really in flight). It is pure: it mutates
// only doc.Steps and returns whether the caller must persist (dirty).
//
// Stage 0 is intentionally skipped here: js-sbom needs the project source on disk,
// so a stuck stage-0 step is recovered by re-running the downloader via
// redriveStageZero, not by resetting the step in place.
//
// reclaimAfter <= 0 with force=false disables reclaim entirely (the live,
// non-reaper callers).
//
// pluginBusy (nil disables the check) reports whether a plugin's dispatch queue
// still holds messages. A stuck step whose queue is non-empty is almost
// certainly waiting its turn behind a deep backlog — its dispatch is not lost,
// and reclaiming it would add a duplicate to the very queue causing the wait
// (mirroring the stage-0 downloader guard in recoverAnalysis). Such steps are
// skipped; a genuinely lost dispatch is reclaimed once the queue drains. force
// ignores the check: at startup the queue is known-empty.
func reclaimStuckSteps(doc *codeclarity.Analysis, now time.Time, reclaimAfter time.Duration, force bool, pluginBusy func(name string) bool) (dirty bool) {
	if !force && reclaimAfter <= 0 {
		return false
	}
	for s := range doc.Steps {
		if s == 0 {
			continue
		}
		for i := range doc.Steps[s] {
			st := &doc.Steps[s][i]
			if st.Status != codeclarity.STARTED {
				continue
			}
			if !force {
				started, perr := time.Parse(time.RFC3339Nano, st.Started_on)
				if perr != nil || now.Sub(started) < reclaimAfter {
					continue
				}
				if pluginBusy != nil && pluginBusy(st.Name) {
					continue
				}
			}
			st.Status = ""
			st.Started_on = ""
			dirty = true
		}
	}
	return dirty
}

// stageReadyPlugins mutates the in-memory analysis (setting ready, not-yet-started
// plugins in the given stage to STARTED) and returns the dispatcher messages that
// should be published once the surrounding transaction commits.
//
// It is purely in-memory: the caller is responsible for persisting `analysis`
// inside its locked transaction and for publishing the returned messages after
// commit. Steps whose status is non-empty are skipped, which makes the whole
// operation idempotent — re-running it never double-dispatches a plugin.
func stageReadyPlugins(analysis *codeclarity.Analysis, stageIndex int, dr *DependencyResolver) ([]pendingMessage, bool) {
	if stageIndex < 0 || stageIndex >= len(analysis.Steps) {
		return nil, false
	}

	var ready []codeclarity.Step
	if dr != nil {
		r, err := dr.GetReadyPlugins(analysis, stageIndex)
		if err != nil {
			log.Printf("GetReadyPlugins(stage %d) failed, falling back to all-in-stage: %v", stageIndex, err)
			r = analysis.Steps[stageIndex]
		}
		ready = dr.TopologicalSort(r)
	} else {
		ready = analysis.Steps[stageIndex]
	}

	readySet := make(map[string]bool, len(ready))
	for _, s := range ready {
		readySet[s.Name] = true
	}

	var msgs []pendingMessage
	staged := false
	for stepId := range analysis.Steps[stageIndex] {
		step := analysis.Steps[stageIndex][stepId]
		// Only dispatch plugins that haven't started yet and are ready.
		if step.Status != "" || !readySet[step.Name] {
			continue
		}

		dispatcherMessage := types_amqp.DispatcherPluginMessage{
			AnalysisId:     analysis.Id,
			OrganizationId: analysis.OrganizationId,
			Data:           analysis.Config,
		}
		data, _ := json.Marshal(dispatcherMessage)
		analysis.Steps[stageIndex][stepId].Status = codeclarity.STARTED
		// Stamp the dispatch time so the reaper can age a STARTED step and reclaim
		// it if its dispatch was lost (the plugin sets its own Started_on on run).
		analysis.Steps[stageIndex][stepId].Started_on = time.Now().Format(time.RFC3339Nano)
		msgs = append(msgs, pendingMessage{queueName: "dispatcher_" + step.Name, data: data, plugin: step.Name, stage: stageIndex})
		staged = true
	}
	return msgs, staged
}

// finalizeOrAdvanceStage atomically reconciles a single analysis. Within one
// transaction it re-reads the row under a `FOR UPDATE` lock and then, based on
// the freshly-locked step statuses:
//
//   - sets FAILURE if any step in the current stage failed;
//   - sets COMPLETED if the last stage is fully successful;
//   - advances the stage if the current stage is complete; and/or
//   - (when allowStaging) dispatches any not-yet-started, dependency-satisfied
//     plugins across stages 0..Stage (this covers the very first dispatch, the
//     newly advanced stage, and plugins whose dependencies just became ready).
//
// It is fully idempotent: terminal analyses are a no-op, and already-started
// plugins are never re-dispatched. The SAME function backs the live message
// handlers and the reaper, so a reaper run racing a live completion is safe —
// whoever wins the row lock makes the transition; the other observes the
// terminal/started state and does nothing.
//
// Returned messages MUST be published by the caller after this returns (the
// transaction has already committed); we never publish under the lock.
//
// allowStageZeroStart gates (re-)dispatch of stage 0. Stage 0 (js-sbom) reads
// the project source from disk, which is only guaranteed present after the
// downloader has run. The live api_request/downloader paths pass true; the
// reaper passes false so it never re-runs js-sbom against missing source (doing
// so would falsely fail the analysis). Later stages read the SBOM from the DB
// and are always safe to (re)dispatch for lost-message recovery.
//
// reclaimAfter (reaper-only; live callers pass 0) recovers a *lost dispatch*: a
// later-stage step left STARTED for longer than reclaimAfter is assumed dead
// (publish failure or a crashed plugin) and reset to "" so the dispatch loop
// re-sends it. forceReclaim (reaper startup only) reclaims every later-stage
// STARTED step regardless of age, since a restart wiped the queue and nothing is
// really in flight. Stage 0 is never reclaimed here — see redriveStageZero.
//
// pluginBusy (reaper-only; live callers pass nil) gates the reclaim per plugin:
// a step whose dispatch queue still holds messages is skipped rather than
// re-dispatched — see reclaimStuckSteps. Ignored on forceReclaim.
//
// It returns an outcome describing the transition made ("completed", "failure",
// "advanced", "dispatched", or "" for no-op) so callers (the reaper) can log it.
func finalizeOrAdvanceStage(analysisId uuid.UUID, db *bun.DB, dr *DependencyResolver, allowStageZeroStart bool, reclaimAfter time.Duration, forceReclaim bool, pluginBusy func(name string) bool) ([]pendingMessage, string, error) {
	ctx := context.Background()
	var messages []pendingMessage
	outcome := ""

	err := db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		doc := &codeclarity.Analysis{Id: analysisId}
		if e := tx.NewSelect().Model(doc).WherePK().For("UPDATE").Scan(ctx); e != nil {
			return fmt.Errorf("lock+reload analysis %s: %w", analysisId, e)
		}

		// Idempotency / transient guards.
		switch doc.Status {
		case codeclarity.COMPLETED, codeclarity.FAILURE, codeclarity.CANCELLED:
			return nil // already terminal
		case codeclarity.UPDATING_DB:
			return nil // packageFollower will re-trigger when the DB update completes
		}
		if len(doc.Steps) == 0 {
			return nil
		}

		// The stage counter already advanced past the last stage but the status
		// was never finalized (a lost COMPLETED/FAILURE write — the original race).
		// Finalize from the terminal step states across all stages.
		if doc.Stage >= len(doc.Steps) {
			anyFailure := false
			for _, stg := range doc.Steps {
				if af, _ := evaluateStage(stg); af {
					anyFailure = true
				}
			}
			if anyFailure {
				doc.Status = codeclarity.FAILURE
				outcome = "failure"
			} else {
				doc.Status = codeclarity.COMPLETED
				outcome = "completed"
			}
			if _, e := tx.NewUpdate().Model(doc).WherePK().Exec(ctx); e != nil {
				return e
			}
			return nil
		}

		dirty := false

		// Reaper-only: reclaim later-stage STARTED steps whose dispatch was lost so
		// the dispatch loop re-sends them. Stage 0 is recovered separately by
		// redriveStageZero (js-sbom needs the source on disk), so it is skipped here.
		if reclaimStuckSteps(doc, time.Now(), reclaimAfter, forceReclaim, pluginBusy) {
			dirty = true
		}

		// Evaluate the current stage from the locked snapshot.
		if doc.Stage >= 0 && doc.Stage < len(doc.Steps) {
			anyFailure, allSuccess := evaluateStage(doc.Steps[doc.Stage])
			switch {
			case anyFailure:
				doc.Status = codeclarity.FAILURE
				outcome = "failure"
				if _, e := tx.NewUpdate().Model(doc).WherePK().Exec(ctx); e != nil {
					return e
				}
				return nil
			case allSuccess:
				doc.Stage++
				dirty = true
				if doc.Stage == len(doc.Steps) {
					doc.Status = codeclarity.COMPLETED
					outcome = "completed"
					if _, e := tx.NewUpdate().Model(doc).WherePK().Exec(ctx); e != nil {
						return e
					}
					return nil
				}
			}
		}

		// Dispatch any plugins that are now ready.
		maxStage := min(doc.Stage, len(doc.Steps)-1)
		for s := 0; s <= maxStage; s++ {
			// Never re-initiate stage 0 from a context that can't guarantee the
			// source is on disk (the reaper). That path belongs to the downloader.
			if s == 0 && !allowStageZeroStart {
				continue
			}
			msgs, staged := stageReadyPlugins(doc, s, dr)
			if staged {
				dirty = true
			}
			messages = append(messages, msgs...)
		}

		if dirty {
			if _, e := tx.NewUpdate().Model(doc).WherePK().Exec(ctx); e != nil {
				return e
			}
		}
		switch {
		case len(messages) > 0:
			outcome = "dispatched"
		case dirty:
			outcome = "advanced"
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return messages, outcome, nil
}

// sendMessages publishes the post-commit messages, logging (not failing) on error.
// A dropped message is recovered by the reaper, so a transient publish failure
// must not panic the consumer.
func sendMessages(service *boilerplates.ServiceBase, msgs []pendingMessage) {
	for _, m := range msgs {
		if err := service.SendMessage(m.queueName, m.data); err != nil {
			log.Printf("Failed to send message to %s: %v", m.queueName, err)
		}
	}
}

// analysisExpired reports whether a non-terminal analysis is older than maxAge and
// should be retired instead of recovered. maxAge <= 0 disables the cap; a zero
// Created_on (unknown age) is never treated as expired, so a row with a missing
// timestamp is never retired defensively.
func analysisExpired(doc *codeclarity.Analysis, now time.Time, maxAge time.Duration) bool {
	if maxAge <= 0 || doc.Created_on.IsZero() {
		return false
	}
	return now.Sub(doc.Created_on) > maxAge
}

// failAnalysis marks a non-terminal analysis FAILURE (status-guarded so it is
// idempotent and never clobbers an already-terminal row). Used to retire analyses
// too old to be worth recovering, so the reaper stops re-driving abandoned work.
func failAnalysis(analysisId uuid.UUID, db *bun.DB) error {
	ctx := context.Background()
	_, err := db.NewUpdate().
		Model((*codeclarity.Analysis)(nil)).
		Set("status = ?", codeclarity.FAILURE).
		Where("id = ?", analysisId).
		Where("status IN (?)", bun.In([]string{
			string(codeclarity.STARTED),
			string(codeclarity.ONGOING),
			string(codeclarity.REQUESTED),
			string(codeclarity.TRIGGERED),
		})).
		Exec(ctx)
	return err
}

// stageZeroNeedsRedrive reports whether a non-terminal, stage-0 analysis has been
// stuck at/before stage 0 long enough to re-drive it (interval reaper). It is true
// when stage 0 is not yet fully successful and the most recent stage-0 activity is
// older than timeout — covering both a pre-download analysis (steps never
// dispatched: age from Created_on) and a stuck STARTED stage-0 step (age from its
// Started_on). redriveStageZero stamps Started_on on re-drive, so this naturally
// throttles repeated re-drives to one per timeout window.
func stageZeroNeedsRedrive(doc *codeclarity.Analysis, now time.Time, timeout time.Duration) bool {
	if doc.Stage != 0 || len(doc.Steps) == 0 {
		return false
	}
	if _, allSuccess := evaluateStage(doc.Steps[0]); allSuccess {
		return false // stage 0 done — the finalizer will advance it, no re-drive
	}
	ref := doc.Created_on
	for _, st := range doc.Steps[0] {
		if st.Started_on == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339Nano, st.Started_on); err == nil && t.After(ref) {
			ref = t
		}
	}
	return now.Sub(ref) >= timeout
}

// redriveStageZero recovers an analysis stranded at or before stage 0 after a
// restart wiped the queue. Stage 0 (js-sbom) reads the project source from disk,
// so it cannot simply be re-dispatched like a later stage — the source must be
// re-fetched first. Under a row lock it resets stage-0 steps (so they are not
// blocked by a stale STARTED) and stamps Started_on as the re-drive time (so the
// interval reaper throttles repeated re-drives). It then either re-sends the
// project to the downloader (VCS / FILE projects) — which calls back into
// dispatchDownloaderResult to start stage 0 — or, for projects needing no
// download, starts stage 0 directly. Idempotent: terminal/advanced analyses are a
// no-op, and re-running the downloader is safe (Git handles an existing checkout).
func redriveStageZero(analysisId uuid.UUID, db *bun.DB, dr *DependencyResolver, service *boilerplates.ServiceBase) error {
	ctx := context.Background()
	var (
		projectId, organizationId uuid.UUID
		proceed                   bool
	)
	now := time.Now().Format(time.RFC3339Nano)
	err := db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		doc := &codeclarity.Analysis{Id: analysisId}
		if e := tx.NewSelect().Model(doc).WherePK().For("UPDATE").Scan(ctx); e != nil {
			return fmt.Errorf("lock+reload analysis %s: %w", analysisId, e)
		}
		switch doc.Status {
		case codeclarity.COMPLETED, codeclarity.FAILURE, codeclarity.CANCELLED, codeclarity.UPDATING_DB:
			return nil // terminal/transient — nothing to re-drive
		}
		if doc.Stage != 0 || len(doc.Steps) == 0 {
			return nil // advanced past stage 0 since selection — let the finalizer handle it
		}
		if _, allSuccess := evaluateStage(doc.Steps[0]); allSuccess {
			return nil // stage 0 already done — the finalizer will advance it
		}
		// Reset stage-0 steps so re-dispatch is not blocked by a stale STARTED, and
		// stamp the re-drive time so the interval reaper does not thrash.
		for i := range doc.Steps[0] {
			doc.Steps[0][i].Status = ""
			doc.Steps[0][i].Started_on = now
			doc.Steps[0][i].Ended_on = ""
		}
		doc.Status = codeclarity.STARTED
		if _, e := tx.NewUpdate().Model(doc).WherePK().Exec(ctx); e != nil {
			return e
		}
		if doc.ProjectId != nil {
			projectId = *doc.ProjectId
		}
		organizationId = doc.OrganizationId
		proceed = true
		return nil
	})
	if err != nil || !proceed {
		return err
	}

	// Decide download vs direct stage-0 start exactly as the live api_request path.
	project := &codeclarity.Project{Id: projectId}
	if e := db.NewSelect().Model(project).WherePK().Scan(ctx); e != nil {
		return fmt.Errorf("redrive: load project %s: %w", projectId, e)
	}
	if project.Type == "FILE" || project.Integration_id != "" {
		var integrationId uuid.UUID
		if project.Integration_id != "" {
			if id, perr := uuid.Parse(project.Integration_id); perr == nil {
				integrationId = id
			} else {
				log.Printf("[redrive] bad integration_id %q on project %s: %v", project.Integration_id, projectId, perr)
			}
		}
		downloaderMessage := types_amqp.DispatcherDownloaderMessage{
			AnalysisId:     analysisId,
			ProjectId:      projectId,
			IntegrationId:  integrationId,
			OrganizationId: organizationId,
		}
		data, _ := json.Marshal(downloaderMessage)
		return service.SendMessage("dispatcher_downloader", data)
	}

	// No download needed — start stage 0 directly.
	msgs, _, e := finalizeOrAdvanceStage(analysisId, db, dr, true, 0, false, nil)
	if e != nil {
		return e
	}
	sendMessages(service, msgs)
	return nil
}

// dispatch routes an incoming message based on its source queue.
//
//   - api_request: initialize the analysis (copy the analyzer's step plan) and
//     either hand off to the downloader (VCS / FILE projects) or start stage 0.
//   - downloader_dispatcher: the source is downloaded — start stage 0.
//   - plugins_dispatcher: a plugin finished — finalize/advance the analysis.
//
// All branches use log-and-return on errors rather than panic(): a panic is
// recovered by the consumer loop and the message requeued, so a deterministically
// failing ("poison") message would otherwise loop forever and starve the queue's
// single consumer. Dropping the message is safe because the reaper reconciles any
// analysis left in a non-terminal state.
func dispatch(connection string, d amqp.Delivery, dependencyResolver *DependencyResolver, service *boilerplates.ServiceBase) {
	switch connection {
	case "api_request":
		dispatchAPIRequest(d, dependencyResolver, service)
	case "downloader_dispatcher":
		dispatchDownloaderResult(d, dependencyResolver, service)
	case "plugins_dispatcher":
		dispatchPluginResult(d, dependencyResolver, service)
	default:
		log.Printf("[dispatch] unknown connection %q, dropping message", connection)
	}
}

func dispatchAPIRequest(d amqp.Delivery, dependencyResolver *DependencyResolver, service *boilerplates.ServiceBase) {
	var rawMessage map[string]any
	if err := json.Unmarshal(d.Body, &rawMessage); err != nil {
		log.Printf("[dispatch:api_request] bad message, dropping: %v", err)
		return
	}
	log.Printf("[dispatch:api_request] received: %+v", rawMessage)

	analysisIdStr, ok := rawMessage["analysis_id"].(string)
	if !ok {
		log.Printf("[dispatch:api_request] analysis_id not a string (%T), dropping", rawMessage["analysis_id"])
		return
	}
	analysisId, err := uuid.Parse(analysisIdStr)
	if err != nil {
		log.Printf("[dispatch:api_request] bad analysis_id %q, dropping: %v", analysisIdStr, err)
		return
	}

	projectIdStr, ok := rawMessage["project_id"].(string)
	if !ok {
		log.Printf("[dispatch:api_request] project_id not a string (%T), dropping", rawMessage["project_id"])
		return
	}
	projectId, err := uuid.Parse(projectIdStr)
	if err != nil {
		log.Printf("[dispatch:api_request] bad project_id %q, dropping: %v", projectIdStr, err)
		return
	}

	organizationIdStr, ok := rawMessage["organization_id"].(string)
	if !ok {
		log.Printf("[dispatch:api_request] organization_id not a string, dropping")
		return
	}
	organizationId, err := uuid.Parse(organizationIdStr)
	if err != nil {
		log.Printf("[dispatch:api_request] bad organization_id %q, dropping: %v", organizationIdStr, err)
		return
	}

	var integrationId uuid.UUID
	if raw := rawMessage["integration_id"]; raw != nil {
		if s, ok := raw.(string); ok && s != "" {
			integrationId, err = uuid.Parse(s)
			if err != nil {
				log.Printf("[dispatch:api_request] bad integration_id %q, dropping: %v", s, err)
				return
			}
		}
	}

	db := service.DB.CodeClarity
	ctx := context.Background()

	// Initialize the analysis from its analyzer's step plan under a row lock.
	analysisDocument := &codeclarity.Analysis{Id: analysisId}
	err = db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		if e := tx.NewSelect().Model(analysisDocument).WherePK().For("UPDATE").Scan(ctx); e != nil {
			return fmt.Errorf("load analysis: %w", e)
		}
		analyzerDocument := &codeclarity.Analyzer{Id: analysisDocument.AnalyzerId}
		if e := tx.NewSelect().Model(analyzerDocument).WherePK().Scan(ctx); e != nil {
			return fmt.Errorf("load analyzer: %w", e)
		}
		analysisDocument.Stage = 0
		analysisDocument.Steps = analyzerDocument.Steps
		analysisDocument.Status = codeclarity.STARTED
		_, e := tx.NewUpdate().Model(analysisDocument).WherePK().Exec(ctx)
		return e
	})
	if err != nil {
		log.Printf("[dispatch:api_request] init failed for %s, dropping: %v", analysisId, err)
		return
	}

	// VCS (integration set) and FILE projects must be downloaded first.
	projectDocument := &codeclarity.Project{Id: projectId}
	if e := db.NewSelect().Model(projectDocument).WherePK().Scan(ctx); e != nil {
		log.Printf("[dispatch:api_request] load project %s failed for %s, dropping: %v", projectId, analysisId, e)
		return
	}

	if integrationId != uuid.Nil || projectDocument.Type == "FILE" {
		downloaderMessage := types_amqp.DispatcherDownloaderMessage{
			AnalysisId:     analysisId,
			ProjectId:      projectId,
			IntegrationId:  integrationId,
			OrganizationId: organizationId,
		}
		data, _ := json.Marshal(downloaderMessage)
		if e := service.SendMessage("dispatcher_downloader", data); e != nil {
			log.Printf("[dispatch:api_request] failed to send to dispatcher_downloader: %v", e)
		}
		return
	}

	// No download needed — start stage 0 directly.
	msgs, _, e := finalizeOrAdvanceStage(analysisId, db, dependencyResolver, true, 0, false, nil)
	if e != nil {
		log.Printf("[dispatch:api_request] stage-0 start failed for %s, dropping (reaper will retry): %v", analysisId, e)
		return
	}
	sendMessages(service, msgs)
}

func dispatchDownloaderResult(d amqp.Delivery, dependencyResolver *DependencyResolver, service *boilerplates.ServiceBase) {
	var msg types_amqp.DownloaderDispatcherMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		log.Printf("[dispatch:downloader_dispatcher] bad message, dropping: %v", err)
		return
	}
	msgs, _, err := finalizeOrAdvanceStage(msg.AnalysisId, service.DB.CodeClarity, dependencyResolver, true, 0, false, nil)
	if err != nil {
		log.Printf("[dispatch:downloader_dispatcher] stage-0 start failed for %s, dropping (reaper will retry): %v", msg.AnalysisId, err)
		return
	}
	sendMessages(service, msgs)
}

func dispatchPluginResult(d amqp.Delivery, dependencyResolver *DependencyResolver, service *boilerplates.ServiceBase) {
	var msg types_amqp.PluginDispatcherMessage
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		log.Printf("[dispatch:plugins_dispatcher] bad message, dropping: %v", err)
		return
	}
	msgs, _, err := finalizeOrAdvanceStage(msg.AnalysisId, service.DB.CodeClarity, dependencyResolver, true, 0, false, nil)
	if err != nil {
		log.Printf("[dispatch:plugins_dispatcher] finalize failed for %s, dropping (reaper will retry): %v", msg.AnalysisId, err)
		return
	}
	sendMessages(service, msgs)
}
