package main

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/CodeClarityCE/utility-boilerplates/testdeps"
	types_amqp "github.com/CodeClarityCE/utility-types/amqp"
	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// analysisTables creates the analysis and result tables the dispatcher reads.
func analysisTables(t *testing.T) *bun.DB {
	t.Helper()
	db := testdeps.Postgres(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewCreateTable().Model((*codeclarity.Analysis)(nil)).Exec(ctx); err != nil {
		t.Fatalf("create analysis table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE result (id uuid PRIMARY KEY DEFAULT uuid_generate_v4(), result jsonb, "analysisId" uuid, plugin varchar, created_on timestamptz DEFAULT now())`); err != nil {
		t.Fatalf("create result table: %v", err)
	}
	return db
}

// defaultAnalyzerResolver declares the finders' dependencies as the default analyzer's plugins do.
func defaultAnalyzerResolver() *DependencyResolver {
	dr := CreateDependencyResolver()
	dr.pluginConfigs["js-sbom"] = pluginDependingOn("js-sbom")
	dr.pluginConfigs["license-finder"] = pluginDependingOn("license-finder", "js-sbom", "php-sbom")
	dr.pluginConfigs["vuln-finder"] = pluginDependingOn("vuln-finder", "js-sbom", "php-sbom")
	return dr
}

// sbomDoneAnalysis inserts an analysis whose js-sbom step has just reported success.
func sbomDoneAnalysis(t *testing.T, db *bun.DB, status codeclarity.AnalysisStatus) uuid.UUID {
	t.Helper()
	sbom := step("js-sbom", codeclarity.SUCCESS)
	sbom.Ended_on = time.Now().Format(time.RFC3339Nano)
	return insertAnalysis(t, db, status, sbom)
}

// insertAnalysis inserts an analysis with the given SBOM steps, followed by the finders.
func insertAnalysis(t *testing.T, db *bun.DB, status codeclarity.AnalysisStatus, sboms ...codeclarity.Step) uuid.UUID {
	t.Helper()
	doc := &codeclarity.Analysis{
		Id:         uuid.New(),
		Created_on: time.Now(),
		Status:     status,
		Config:     map[string]any{},
		Steps: [][]codeclarity.Step{
			sboms,
			{step("license-finder", ""), step("vuln-finder", "")},
		},
	}
	if _, err := db.NewInsert().Model(doc).Exec(context.Background()); err != nil {
		t.Fatalf("insert analysis: %v", err)
	}
	return doc.Id
}

// followerFinishes writes what the package follower writes when its import is done.
func followerFinishes(t *testing.T, db *bun.DB, id uuid.UUID, language string) {
	t.Helper()
	_, err := db.NewUpdate().Model((*codeclarity.Analysis)(nil)).
		Set("status = CASE WHEN status = ? THEN ? ELSE status END", codeclarity.UPDATING_DB, codeclarity.ONGOING).
		Set("config = jsonb_set(COALESCE(config, '{}'::jsonb), '{"+packageImportDoneKey+"}', COALESCE(config->'"+packageImportDoneKey+"', '{}'::jsonb) || jsonb_build_object(?::text, ?::text))", language, time.Now().Format(time.RFC3339Nano)).
		Where("id = ?", id).
		Exec(context.Background())
	if err != nil {
		t.Fatalf("follower finish: %v", err)
	}
}

func dispatchedQueues(msgs []pendingMessage) map[string]bool {
	queues := map[string]bool{}
	for _, m := range msgs {
		queues[m.queueName] = true
	}
	return queues
}

func TestFindersWaitForThePackageFollower(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status codeclarity.AnalysisStatus
	}{
		{"SBOM completion handled before the follower started", codeclarity.ONGOING},
		{"SBOM completion handled while the follower runs", codeclarity.UPDATING_DB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := analysisTables(t)
			dr := defaultAnalyzerResolver()
			id := sbomDoneAnalysis(t, db, tc.status)

			// The SBOM plugin's completion message arrives first.
			msgs, _, err := finalizeOrAdvanceStage(id, db, dr, true, 0, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := dispatchedQueues(msgs); got["dispatcher_license-finder"] || got["dispatcher_vuln-finder"] {
				t.Fatalf("finders dispatched before the package import was done: %v", got)
			}

			// The follower finishes and notifies the dispatcher.
			followerFinishes(t, db, id, "javascript")
			msgs, _, err = finalizeOrAdvanceStage(id, db, dr, true, 0, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			got := dispatchedQueues(msgs)
			if !got["dispatcher_license-finder"] || !got["dispatcher_vuln-finder"] {
				t.Fatalf("finders not dispatched once the package import is done: %v", got)
			}
		})
	}
}

// sbomCommits records an SBOM step's success, as the plugin does after its run.
func sbomCommits(t *testing.T, db *bun.DB, id uuid.UUID, name string) {
	t.Helper()
	doc := loadAnalysis(t, db, id)
	for i := range doc.Steps[0] {
		if doc.Steps[0][i].Name == name {
			doc.Steps[0][i].Status = codeclarity.SUCCESS
		}
	}
	if _, err := db.NewUpdate().Model(doc).Column("steps").WherePK().Exec(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFindersWaitForTheImportOfEverySbom(t *testing.T) {
	db := analysisTables(t)
	dr := defaultAnalyzerResolver()
	dr.pluginConfigs["php-sbom"] = pluginDependingOn("php-sbom")
	id := insertAnalysis(t, db, codeclarity.ONGOING, step("js-sbom", codeclarity.SUCCESS), step("php-sbom", codeclarity.SUCCESS))

	followerFinishes(t, db, id, "javascript")
	msgs, _, err := finalizeOrAdvanceStage(id, db, dr, true, 0, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := dispatchedQueues(msgs); got["dispatcher_license-finder"] || got["dispatcher_vuln-finder"] {
		t.Fatalf("finders dispatched while the PHP import was still running: %v", got)
	}

	followerFinishes(t, db, id, "php")
	assertFindersDispatched(t, db, dr, id)
}

func TestFollowerFinishingBeforeTheSbomCommitsLetsTheFindersRun(t *testing.T) {
	db := analysisTables(t)
	dr := defaultAnalyzerResolver()
	id := insertAnalysis(t, db, codeclarity.ONGOING, step("js-sbom", codeclarity.STARTED))

	// The follower's import ends while js-sbom is still writing its step.
	followerFinishes(t, db, id, "javascript")
	msgs, _, err := finalizeOrAdvanceStage(id, db, dr, true, 0, false, nil)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("msgs = %v, err = %v; want nothing dispatched before the SBOM succeeds", msgs, err)
	}

	sbomCommits(t, db, id, "js-sbom")
	assertFindersDispatched(t, db, dr, id)
}

// sentMessage is one message a recovery published.
type sentMessage struct {
	queue string
	body  types_amqp.SbomPackageFollowerMessage
}

// recordSends returns a sender that keeps what it is given.
func recordSends(t *testing.T, sent *[]sentMessage) func(string, []byte) error {
	return func(queue string, data []byte) error {
		var body types_amqp.SbomPackageFollowerMessage
		if err := json.Unmarshal(data, &body); err != nil {
			t.Fatalf("unreadable follower message: %v", err)
		}
		*sent = append(*sent, sentMessage{queue, body})
		return nil
	}
}

// storeSbom saves an SBOM result with the given packages and points js-sbom at it.
func storeSbom(t *testing.T, db *bun.DB, id uuid.UUID, packages ...string) {
	t.Helper()
	ctx := context.Background()
	deps := map[string]any{}
	for _, name := range packages {
		deps[name] = map[string]any{"1.0.0": map[string]any{}}
	}
	raw, _ := json.Marshal(map[string]any{"workspaces": map[string]any{".": map[string]any{"dependencies": deps}}})
	key := uuid.New()
	if _, err := db.ExecContext(ctx, `INSERT INTO result (id, result, "analysisId", plugin) VALUES (?, ?, ?, 'js-sbom')`, key, string(raw), id); err != nil {
		t.Fatalf("store sbom: %v", err)
	}
	doc := &codeclarity.Analysis{Id: id}
	if err := db.NewSelect().Model(doc).WherePK().Scan(ctx); err != nil {
		t.Fatal(err)
	}
	doc.Steps[0][0].Result = map[string]any{"sbomKey": key.String()}
	if _, err := db.NewUpdate().Model(doc).Column("steps").WherePK().Exec(ctx); err != nil {
		t.Fatal(err)
	}
}

// advancedPastSbom handles the SBOM's completion, moving the analysis to stage 1.
func advancedPastSbom(t *testing.T, db *bun.DB, dr *DependencyResolver, id uuid.UUID) {
	t.Helper()
	if _, _, err := finalizeOrAdvanceStage(id, db, dr, true, 0, false, nil); err != nil {
		t.Fatal(err)
	}
}

// assertFindersDispatched runs the finalizer and checks both finders are sent.
func assertFindersDispatched(t *testing.T, db *bun.DB, dr *DependencyResolver, id uuid.UUID) {
	t.Helper()
	msgs, _, err := finalizeOrAdvanceStage(id, db, dr, false, 0, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := dispatchedQueues(msgs); !got["dispatcher_license-finder"] || !got["dispatcher_vuln-finder"] {
		t.Fatalf("finders not dispatched: %v", got)
	}
}

func TestLostFollowerMessageIsResentAndTheFindersThenRun(t *testing.T) {
	db := analysisTables(t)
	dr := defaultAnalyzerResolver()
	id := sbomDoneAnalysis(t, db, codeclarity.ONGOING)
	storeSbom(t, db, id, "lodash", "react")
	advancedPastSbom(t, db, dr, id)

	// The dispatcher restarts: the follower's message was lost with the queue.
	var sent []sentMessage
	outcome, err := recoverPackageImport(id, db, recordSends(t, &sent), 30*time.Minute, true)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != updatingDBImportResent || len(sent) != 1 {
		t.Fatalf("outcome = %q, sent = %+v; want the import sent again", outcome, sent)
	}
	if sent[0].queue != packageFollowerQueue || sent[0].body.Language != "javascript" ||
		!slices.Equal(sent[0].body.PackagesNames, []string{"lodash", "react"}) || sent[0].body.AnalysisId != id {
		t.Fatalf("unexpected follower message %+v", sent[0])
	}

	// The re-sent import completes and the finders are dispatched.
	followerFinishes(t, db, id, "javascript")
	assertFindersDispatched(t, db, dr, id)
}

func TestIntervalRecoveryWaitsForTheStuckThreshold(t *testing.T) {
	db := analysisTables(t)
	dr := defaultAnalyzerResolver()
	id := sbomDoneAnalysis(t, db, codeclarity.ONGOING)
	storeSbom(t, db, id, "lodash")
	advancedPastSbom(t, db, dr, id)

	var sent []sentMessage
	outcome, err := recoverPackageImport(id, db, recordSends(t, &sent), 30*time.Minute, false)
	if err != nil || outcome != "" || len(sent) != 0 {
		t.Fatalf("outcome = %q, sent = %+v, err = %v; want a fresh wait left alone", outcome, sent, err)
	}

	// Past the threshold the interval pass re-sends it, then waits a full threshold again.
	outcome, err = recoverPackageImport(id, db, recordSends(t, &sent), time.Nanosecond, false)
	if err != nil || outcome != updatingDBImportResent || len(sent) != 1 {
		t.Fatalf("outcome = %q, sent = %+v, err = %v; want the import sent again", outcome, sent, err)
	}
	outcome, _ = recoverPackageImport(id, db, recordSends(t, &sent), 30*time.Minute, false)
	if outcome != "" || len(sent) != 1 {
		t.Fatalf("outcome = %q, sent = %d; want no second send inside the threshold", outcome, len(sent))
	}
}

// loadAnalysis reads an analysis back from the database.
func loadAnalysis(t *testing.T, db *bun.DB, id uuid.UUID) *codeclarity.Analysis {
	t.Helper()
	doc := &codeclarity.Analysis{Id: id}
	if err := db.NewSelect().Model(doc).WherePK().Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestAStoppedFollowerFailsTheWaitingAnalysisAfterTheGrace(t *testing.T) {
	db := analysisTables(t)
	dr := defaultAnalyzerResolver()
	id := sbomDoneAnalysis(t, db, codeclarity.ONGOING)
	advancedPastSbom(t, db, dr, id)

	noFollower := func(name string) (int, error) {
		if name != packageFollowerName {
			t.Fatalf("probed %q, want only the package follower", name)
		}
		return 0, nil
	}
	watch := newPluginWatch()
	t0 := time.Now()
	if checkStalledPlugins(id, db, noFollower, watch, t0, testGrace) {
		t.Fatal("failed before the grace period ended")
	}
	if !checkStalledPlugins(id, db, noFollower, watch, t0.Add(testGrace), testGrace) {
		t.Fatal("not failed once the follower had no consumer for the grace period")
	}
	doc := loadAnalysis(t, db, id)
	if doc.Status != codeclarity.FAILURE || !strings.Contains(doc.FailureReason, "package follower is not running") {
		t.Fatalf("status = %q, reason = %q; want a failure naming the package follower", doc.Status, doc.FailureReason)
	}
}

func TestARunningFollowerIsNotAStall(t *testing.T) {
	db := analysisTables(t)
	id := sbomDoneAnalysis(t, db, codeclarity.UPDATING_DB)
	oneFollower := func(string) (int, error) { return 1, nil }
	watch := newPluginWatch()
	t0 := time.Now()
	for _, at := range []time.Time{t0, t0.Add(10 * testGrace)} {
		if checkStalledPlugins(id, db, oneFollower, watch, at, testGrace) {
			t.Fatal("a follower with a consumer was treated as stopped")
		}
	}
}

func TestAnAbandonedWaitIsRetiredAtTheAgeCap(t *testing.T) {
	db := analysisTables(t)
	dr := defaultAnalyzerResolver()
	id := sbomDoneAnalysis(t, db, codeclarity.ONGOING)
	advancedPastSbom(t, db, dr, id)
	if _, err := db.NewUpdate().Model((*codeclarity.Analysis)(nil)).Set("created_on = ?", time.Now().Add(-25*time.Hour)).Where("id = ?", id).Exec(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !recoverAnalysis(id, db, dr, nil, 30*time.Minute, 24*time.Hour, false) {
		t.Fatal("an analysis waiting past RECOVERY_MAX_AGE was not retired")
	}
	if doc := loadAnalysis(t, db, id); doc.Status != codeclarity.FAILURE {
		t.Fatalf("status = %q, want failure", doc.Status)
	}
}

func TestSbomWithoutPackagesNeedsNoImport(t *testing.T) {
	db := analysisTables(t)
	dr := defaultAnalyzerResolver()
	id := sbomDoneAnalysis(t, db, codeclarity.ONGOING)
	storeSbom(t, db, id)
	advancedPastSbom(t, db, dr, id)

	var sent []sentMessage
	outcome, err := recoverPackageImport(id, db, recordSends(t, &sent), 30*time.Minute, true)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != updatingDBResumed || len(sent) != 0 {
		t.Fatalf("outcome = %q, sent = %+v; want the wait ended without an import", outcome, sent)
	}
	assertFindersDispatched(t, db, dr, id)
}
