package main

import (
	"slices"
	"testing"
	"time"

	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
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
