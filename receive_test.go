package main

import (
	"testing"
	"time"

	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
)

func step(name string, status codeclarity.AnalysisStatus) codeclarity.Step {
	return codeclarity.Step{Name: name, Status: status}
}

func TestEvaluateStage(t *testing.T) {
	cases := []struct {
		name        string
		steps       []codeclarity.Step
		wantFailure bool
		wantSuccess bool
	}{
		{
			name:        "all success",
			steps:       []codeclarity.Step{step("a", codeclarity.SUCCESS), step("b", codeclarity.SUCCESS)},
			wantFailure: false,
			wantSuccess: true,
		},
		{
			name:        "one failure",
			steps:       []codeclarity.Step{step("a", codeclarity.SUCCESS), step("b", codeclarity.FAILURE)},
			wantFailure: true,
			wantSuccess: false,
		},
		{
			name:        "mixed success and started",
			steps:       []codeclarity.Step{step("a", codeclarity.SUCCESS), step("b", codeclarity.STARTED)},
			wantFailure: false,
			wantSuccess: false,
		},
		{
			name:        "all empty (never dispatched)",
			steps:       []codeclarity.Step{step("a", ""), step("b", "")},
			wantFailure: false,
			wantSuccess: false,
		},
		{
			name:        "empty stage is vacuously complete",
			steps:       []codeclarity.Step{},
			wantFailure: false,
			wantSuccess: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			anyFailure, allSuccess := evaluateStage(tc.steps)
			if anyFailure != tc.wantFailure {
				t.Errorf("anyFailure = %v, want %v", anyFailure, tc.wantFailure)
			}
			if allSuccess != tc.wantSuccess {
				t.Errorf("allSuccess = %v, want %v", allSuccess, tc.wantSuccess)
			}
		})
	}
}

func TestStageReadyPlugins_DispatchesEmptyOnly(t *testing.T) {
	a := &codeclarity.Analysis{
		Id:             uuid.New(),
		OrganizationId: uuid.New(),
		Stage:          0,
		Steps: [][]codeclarity.Step{
			{step("js-sbom", ""), step("license-finder", "")},
		},
	}

	// dr == nil → fall back to all-in-stage.
	msgs, staged := stageReadyPlugins(a, 0, nil)
	if !staged {
		t.Fatal("expected staged=true")
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	want := map[string]bool{"dispatcher_js-sbom": true, "dispatcher_license-finder": true}
	for _, m := range msgs {
		if !want[m.queueName] {
			t.Errorf("unexpected queue %q", m.queueName)
		}
	}
	// Steps must be marked STARTED in memory.
	for _, s := range a.Steps[0] {
		if s.Status != codeclarity.STARTED {
			t.Errorf("step %q status = %q, want STARTED", s.Name, s.Status)
		}
	}
}

func TestStageReadyPlugins_Idempotent(t *testing.T) {
	a := &codeclarity.Analysis{
		Id:    uuid.New(),
		Stage: 0,
		Steps: [][]codeclarity.Step{
			{step("js-sbom", codeclarity.SUCCESS), step("license-finder", codeclarity.STARTED)},
		},
	}
	// Nothing is empty → nothing should be (re)dispatched.
	msgs, staged := stageReadyPlugins(a, 0, nil)
	if staged || len(msgs) != 0 {
		t.Fatalf("expected no dispatch for already-started/done steps, got staged=%v msgs=%d", staged, len(msgs))
	}
}

func TestStageReadyPlugins_StageOutOfRange(t *testing.T) {
	a := &codeclarity.Analysis{Steps: [][]codeclarity.Step{{step("a", "")}}}
	if msgs, staged := stageReadyPlugins(a, 5, nil); staged || msgs != nil {
		t.Fatalf("out-of-range stage should be a no-op, got staged=%v msgs=%v", staged, msgs)
	}
}

func TestStageReadyPlugins_StampsStartedOn(t *testing.T) {
	a := &codeclarity.Analysis{
		Id:    uuid.New(),
		Stage: 0,
		Steps: [][]codeclarity.Step{{step("js-sbom", "")}},
	}
	if _, staged := stageReadyPlugins(a, 0, nil); !staged {
		t.Fatal("expected staged=true")
	}
	if a.Steps[0][0].Started_on == "" {
		t.Fatal("expected Started_on to be stamped on dispatch")
	}
	if _, err := time.Parse(time.RFC3339Nano, a.Steps[0][0].Started_on); err != nil {
		t.Fatalf("Started_on not RFC3339Nano: %v", err)
	}
}

func TestReclaimStuckSteps(t *testing.T) {
	const timeout = 30 * time.Minute
	now := time.Now()
	stale := now.Add(-time.Hour).Format(time.RFC3339Nano)
	fresh := now.Add(-time.Minute).Format(time.RFC3339Nano)

	t.Run("disabled when reclaimAfter<=0", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "a", Status: codeclarity.STARTED, Started_on: stale}},
		}}
		if fz, dirty := reclaimStuckSteps(doc, now, 0); fz || dirty {
			t.Fatalf("reclaim must be a no-op when disabled, got fz=%v dirty=%v", fz, dirty)
		}
		if doc.Steps[0][0].Status != codeclarity.STARTED {
			t.Fatal("step must be untouched when disabled")
		}
	})

	t.Run("stale stage-0 step fails the analysis", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.STARTED, Started_on: stale}},
		}}
		fz, dirty := reclaimStuckSteps(doc, now, timeout)
		if !fz || dirty {
			t.Fatalf("expected failStageZero=true dirty=false, got fz=%v dirty=%v", fz, dirty)
		}
	})

	t.Run("stale later-stage step is reset for re-dispatch", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.SUCCESS}},
			{{Name: "vuln-finder", Status: codeclarity.STARTED, Started_on: stale}},
		}}
		fz, dirty := reclaimStuckSteps(doc, now, timeout)
		if fz || !dirty {
			t.Fatalf("expected failStageZero=false dirty=true, got fz=%v dirty=%v", fz, dirty)
		}
		if doc.Steps[1][0].Status != "" || doc.Steps[1][0].Started_on != "" {
			t.Fatalf("stale later-stage step must be cleared, got %+v", doc.Steps[1][0])
		}
	})

	t.Run("fresh and non-started steps are left alone", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.SUCCESS}},
			{{Name: "vuln-finder", Status: codeclarity.STARTED, Started_on: fresh}},
		}}
		if fz, dirty := reclaimStuckSteps(doc, now, timeout); fz || dirty {
			t.Fatalf("fresh STARTED step must be untouched, got fz=%v dirty=%v", fz, dirty)
		}
	})
}
