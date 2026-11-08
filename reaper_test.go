package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func TestDropSucceededSteps(t *testing.T) {
	t.Run("drops a dispatch whose step turned SUCCESS", func(t *testing.T) {
		doc := &codeclarity.Analysis{
			Id: uuid.New(),
			Steps: [][]codeclarity.Step{
				{step("js-sbom", codeclarity.SUCCESS)},
				{step("vuln-finder", codeclarity.SUCCESS), step("license-finder", codeclarity.STARTED)},
			},
		}
		msgs := []pendingMessage{
			{queueName: "dispatcher_vuln-finder", plugin: "vuln-finder", stage: 1},
			{queueName: "dispatcher_license-finder", plugin: "license-finder", stage: 1},
		}
		got := dropSucceededSteps(doc, msgs)
		if len(got) != 1 || got[0].plugin != "license-finder" {
			t.Fatalf("expected only license-finder kept, got %+v", got)
		}
	})

	t.Run("keeps everything when no step is SUCCESS", func(t *testing.T) {
		doc := &codeclarity.Analysis{
			Id: uuid.New(),
			Steps: [][]codeclarity.Step{
				{step("js-sbom", codeclarity.SUCCESS)},
				{step("vuln-finder", codeclarity.STARTED)},
			},
		}
		msgs := []pendingMessage{{queueName: "dispatcher_vuln-finder", plugin: "vuln-finder", stage: 1}}
		if got := dropSucceededSteps(doc, msgs); len(got) != 1 {
			t.Fatalf("expected message kept, got %+v", got)
		}
	})

	t.Run("SUCCESS in another stage does not shadow the dispatched step", func(t *testing.T) {
		doc := &codeclarity.Analysis{
			Id: uuid.New(),
			Steps: [][]codeclarity.Step{
				{step("js-sbom", codeclarity.SUCCESS)},
				{step("vuln-finder", codeclarity.SUCCESS)},
				{step("vuln-finder", codeclarity.STARTED)},
			},
		}
		msgs := []pendingMessage{{queueName: "dispatcher_vuln-finder", plugin: "vuln-finder", stage: 2}}
		if got := dropSucceededSteps(doc, msgs); len(got) != 1 {
			t.Fatalf("stage-2 dispatch must not be dropped for a stage-1 SUCCESS, got %+v", got)
		}
	})

	t.Run("out-of-range stage is kept", func(t *testing.T) {
		doc := &codeclarity.Analysis{Id: uuid.New(), Steps: [][]codeclarity.Step{{step("js-sbom", codeclarity.SUCCESS)}}}
		msgs := []pendingMessage{{queueName: "dispatcher_vuln-finder", plugin: "vuln-finder", stage: 5}}
		if got := dropSucceededSteps(doc, msgs); len(got) != 1 {
			t.Fatalf("out-of-range stage must publish as-is, got %+v", got)
		}
	})
}

func TestResumeStuckUpdatingDB(t *testing.T) {
	const timeout = 30 * time.Minute
	now := time.Now()
	ago := func(d time.Duration) string { return now.Add(-d).Format(time.RFC3339Nano) }

	updating := func(created time.Duration, steps ...codeclarity.Step) *codeclarity.Analysis {
		return &codeclarity.Analysis{
			Status:     codeclarity.UPDATING_DB,
			Created_on: now.Add(-created),
			Steps:      [][]codeclarity.Step{steps},
		}
	}

	t.Run("startup pass resumes a fresh analysis", func(t *testing.T) {
		doc := updating(time.Minute, step("js-sbom", codeclarity.SUCCESS))
		if !resumeStuckUpdatingDB(doc, now, timeout, true) {
			t.Fatal("startup must resume: the queue is known to be empty")
		}
		if doc.Status != codeclarity.ONGOING {
			t.Fatalf("status = %q, want ongoing", doc.Status)
		}
	})

	t.Run("interval pass leaves a recently active analysis alone", func(t *testing.T) {
		st := step("js-sbom", codeclarity.SUCCESS)
		st.Ended_on = ago(5 * time.Minute)
		doc := updating(2*time.Hour, st)
		if resumeStuckUpdatingDB(doc, now, timeout, false) {
			t.Fatal("must not resume within the stuck threshold")
		}
		if doc.Status != codeclarity.UPDATING_DB {
			t.Fatalf("status changed to %q", doc.Status)
		}
	})

	t.Run("interval pass resumes after the stuck threshold", func(t *testing.T) {
		st := step("js-sbom", codeclarity.SUCCESS)
		st.Ended_on = ago(31 * time.Minute)
		doc := updating(2*time.Hour, st)
		if !resumeStuckUpdatingDB(doc, now, timeout, false) {
			t.Fatal("must resume past the stuck threshold")
		}
	})

	t.Run("activity in any stage counts", func(t *testing.T) {
		old := step("js-sbom", codeclarity.SUCCESS)
		old.Ended_on = ago(time.Hour)
		recent := step("vuln-finder", codeclarity.STARTED)
		recent.Started_on = ago(time.Minute)
		doc := &codeclarity.Analysis{
			Status:     codeclarity.UPDATING_DB,
			Created_on: now.Add(-2 * time.Hour),
			Steps:      [][]codeclarity.Step{{old}, {recent}},
		}
		if resumeStuckUpdatingDB(doc, now, timeout, false) {
			t.Fatal("a later stage's recent activity must hold off recovery")
		}
	})

	t.Run("repeating recovery changes nothing", func(t *testing.T) {
		doc := updating(time.Hour, step("js-sbom", codeclarity.SUCCESS))
		if !resumeStuckUpdatingDB(doc, now, timeout, true) {
			t.Fatal("first recovery must resume")
		}
		if resumeStuckUpdatingDB(doc, now, timeout, true) {
			t.Fatal("second recovery must be a no-op")
		}
		if doc.Status != codeclarity.ONGOING {
			t.Fatalf("status = %q, want ongoing", doc.Status)
		}
	})

	t.Run("other statuses are not touched", func(t *testing.T) {
		for _, status := range []codeclarity.AnalysisStatus{codeclarity.COMPLETED, codeclarity.FAILURE, codeclarity.CANCELLED, codeclarity.STARTED} {
			doc := &codeclarity.Analysis{Status: status, Created_on: now.Add(-time.Hour)}
			if resumeStuckUpdatingDB(doc, now, timeout, true) || doc.Status != status {
				t.Fatalf("status %q must be left alone, got %q", status, doc.Status)
			}
		}
	})
}

func TestRecoverableStatusesIncludeUpdatingDB(t *testing.T) {
	want := []string{"ongoing", "started", "requested", "triggered", "updating_db"}
	if !slices.Equal(recoverableStatuses, want) {
		t.Fatalf("recoverableStatuses = %v, want %v", recoverableStatuses, want)
	}
}

// recordingConnector captures each statement so writes can be checked without a database.
type recordingConnector struct{ queries *[]string }

func (c recordingConnector) Connect(context.Context) (driver.Conn, error) {
	return recordingConn(c), nil
}
func (recordingConnector) Driver() driver.Driver { return nil }

type recordingConn struct{ queries *[]string }

func (recordingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (recordingConn) Close() error                        { return nil }
func (recordingConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (c recordingConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	*c.queries = append(*c.queries, query)
	return driver.RowsAffected(1), nil
}

func TestRetiringAnAnalysisSaysItWasInterrupted(t *testing.T) {
	var queries []string
	db := bun.NewDB(sql.OpenDB(recordingConnector{&queries}), pgdialect.New())
	defer db.Close()

	if err := failAnalysis(uuid.New(), db); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 1 {
		t.Fatalf("expected one write, got %v", queries)
	}
	if !strings.Contains(queries[0], "interrupted") || !strings.Contains(queries[0], "resubmit") {
		t.Errorf("reason does not say the analysis was interrupted: %s", queries[0])
	}
	if !strings.Contains(queries[0], "COALESCE(failure_reason") {
		t.Errorf("an earlier reason could be overwritten: %s", queries[0])
	}
}

func TestUpdatingDBStuckMeasuresFromTheUpdateStart(t *testing.T) {
	const timeout = 30 * time.Minute
	now := time.Now()
	stamp := func(d time.Duration) string { return now.Add(-d).Format(time.RFC3339Nano) }
	doc := func(started string, lastStep time.Duration) *codeclarity.Analysis {
		st := step("js-sbom", codeclarity.SUCCESS)
		st.Ended_on = stamp(lastStep)
		return &codeclarity.Analysis{
			Status:     codeclarity.UPDATING_DB,
			Created_on: now.Add(-2 * time.Hour),
			Config:     map[string]any{updatingDBStartedKey: started},
			Steps:      [][]codeclarity.Step{{st}},
		}
	}

	if updatingDBStuck(doc(stamp(5*time.Minute), time.Hour), now, timeout, false) {
		t.Error("an update that began recently must be left alone, whatever the step age")
	}
	if !updatingDBStuck(doc(stamp(time.Hour), time.Minute), now, timeout, false) {
		t.Error("an update that began long ago is stuck, even with recent step activity")
	}
	if !updatingDBStuck(doc("not a time", time.Hour), now, timeout, false) {
		t.Error("an unreadable start falls back to step activity")
	}
}

func TestSbomsToReimport(t *testing.T) {
	jsKey, phpKey := uuid.New(), uuid.New()
	js := step("js-sbom", codeclarity.SUCCESS)
	js.Result = map[string]any{"sbomKey": jsKey.String()}
	php := step("php-sbom", codeclarity.SUCCESS)
	php.Result = map[string]any{"sbomKey": phpKey.String()}
	unfinished := step("php-sbom", codeclarity.STARTED)
	other := step("vuln-finder", codeclarity.SUCCESS)

	got := sbomsToReimport([][]codeclarity.Step{{js, unfinished}, {php, other}})
	want := []sbomImport{{jsKey, "javascript"}, {phpKey, "php"}}
	if !slices.Equal(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPackageNamesFromSbom(t *testing.T) {
	raw := []byte(`{"workspaces":{
		"a":{"dependencies":{"lodash":{"4.0.0":{}},"react":{"18.0.0":{}}}},
		"b":{"dependencies":{"lodash":{"3.0.0":{}},"vue":{"3.0.0":{}}}}
	}}`)
	got, err := packageNamesFromSbom(raw)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"lodash", "react", "vue"}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if _, err := packageNamesFromSbom([]byte("nope")); err == nil {
		t.Fatal("a corrupt SBOM must be an error, not an empty import")
	}
}
