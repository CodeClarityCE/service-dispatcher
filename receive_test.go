package main

import (
	"reflect"
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
			name:        "skipped steps do not block the stage",
			steps:       []codeclarity.Step{step("js-sbom", codeclarity.SKIPPED), step("php-sbom", codeclarity.SUCCESS)},
			wantFailure: false,
			wantSuccess: true,
		},
		{
			name:        "a selected plugin's failure still fails a stage with skips",
			steps:       []codeclarity.Step{step("js-sbom", codeclarity.SKIPPED), step("php-sbom", codeclarity.FAILURE)},
			wantFailure: true,
			wantSuccess: false,
		},
		{
			name:        "a running plugin still holds a stage with skips",
			steps:       []codeclarity.Step{step("js-sbom", codeclarity.SKIPPED), step("php-sbom", codeclarity.STARTED)},
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

	t.Run("disabled when reclaimAfter<=0 and not forced", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.SUCCESS}},
			{{Name: "vuln-finder", Status: codeclarity.STARTED, Started_on: stale}},
		}}
		if dirty := reclaimStuckSteps(doc, now, 0, false, nil); dirty {
			t.Fatalf("reclaim must be a no-op when disabled, got dirty=%v", dirty)
		}
		if doc.Steps[1][0].Status != codeclarity.STARTED {
			t.Fatal("step must be untouched when disabled")
		}
	})

	t.Run("stage-0 steps are never reclaimed in place", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.STARTED, Started_on: stale}},
		}}
		if dirty := reclaimStuckSteps(doc, now, timeout, false, nil); dirty {
			t.Fatalf("stage 0 must be skipped by reclaim, got dirty=%v", dirty)
		}
		if doc.Steps[0][0].Status != codeclarity.STARTED {
			t.Fatal("stage-0 step must be left for redriveStageZero, not cleared")
		}
	})

	t.Run("stale later-stage step is reset for re-dispatch", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.SUCCESS}},
			{{Name: "vuln-finder", Status: codeclarity.STARTED, Started_on: stale}},
		}}
		if dirty := reclaimStuckSteps(doc, now, timeout, false, nil); !dirty {
			t.Fatalf("expected dirty=true")
		}
		if doc.Steps[1][0].Status != "" || doc.Steps[1][0].Started_on != "" {
			t.Fatalf("stale later-stage step must be cleared, got %+v", doc.Steps[1][0])
		}
	})

	t.Run("fresh later-stage step is left alone unless forced", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.SUCCESS}},
			{{Name: "vuln-finder", Status: codeclarity.STARTED, Started_on: fresh}},
		}}
		if dirty := reclaimStuckSteps(doc, now, timeout, false, nil); dirty {
			t.Fatalf("fresh STARTED step must be untouched, got dirty=%v", dirty)
		}
	})

	t.Run("force reclaims a fresh later-stage step regardless of age", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.SUCCESS}},
			{{Name: "vuln-finder", Status: codeclarity.STARTED, Started_on: fresh}},
		}}
		if dirty := reclaimStuckSteps(doc, now, 0, true, nil); !dirty {
			t.Fatalf("force must reclaim regardless of age, got dirty=%v", dirty)
		}
		if doc.Steps[1][0].Status != "" || doc.Steps[1][0].Started_on != "" {
			t.Fatalf("forced reclaim must clear the step, got %+v", doc.Steps[1][0])
		}
		// Even when forced, stage 0 stays put.
		if doc.Steps[0][0].Status != codeclarity.SUCCESS {
			t.Fatal("stage 0 must remain untouched even when forced")
		}
	})

	t.Run("busy plugin queue defers reclaim of a stale step", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.SUCCESS}},
			{{Name: "vuln-finder", Status: codeclarity.STARTED, Started_on: stale}},
		}}
		var probed []string
		busy := func(name string) bool { probed = append(probed, name); return true }
		if dirty := reclaimStuckSteps(doc, now, timeout, false, busy); dirty {
			t.Fatalf("busy queue must defer reclaim, got dirty=%v", dirty)
		}
		if doc.Steps[1][0].Status != codeclarity.STARTED {
			t.Fatal("step must stay STARTED while its queue holds messages")
		}
		if len(probed) != 1 || probed[0] != "vuln-finder" {
			t.Fatalf("expected a single probe for vuln-finder, got %v", probed)
		}
	})

	t.Run("busy check only runs for stale steps", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.SUCCESS}},
			{{Name: "vuln-finder", Status: codeclarity.STARTED, Started_on: fresh}},
		}}
		busy := func(string) bool { t.Fatal("fresh step must not probe the queue"); return true }
		if dirty := reclaimStuckSteps(doc, now, timeout, false, busy); dirty {
			t.Fatalf("fresh STARTED step must be untouched, got dirty=%v", dirty)
		}
	})

	t.Run("force ignores the busy check", func(t *testing.T) {
		doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{
			{{Name: "js-sbom", Status: codeclarity.SUCCESS}},
			{{Name: "vuln-finder", Status: codeclarity.STARTED, Started_on: stale}},
		}}
		busy := func(string) bool { return true }
		if dirty := reclaimStuckSteps(doc, now, 0, true, busy); !dirty {
			t.Fatalf("force must reclaim even with a busy queue, got dirty=%v", dirty)
		}
		if doc.Steps[1][0].Status != "" {
			t.Fatalf("forced reclaim must clear the step, got %+v", doc.Steps[1][0])
		}
	})
}

func TestAnalysisExpired(t *testing.T) {
	const maxAge = 24 * time.Hour
	now := time.Now()

	t.Run("older than maxAge is expired", func(t *testing.T) {
		doc := &codeclarity.Analysis{Created_on: now.Add(-25 * time.Hour)}
		if !analysisExpired(doc, now, maxAge) {
			t.Fatal("expected expired for a 25h-old analysis")
		}
	})

	t.Run("younger than maxAge is not expired", func(t *testing.T) {
		doc := &codeclarity.Analysis{Created_on: now.Add(-1 * time.Hour)}
		if analysisExpired(doc, now, maxAge) {
			t.Fatal("a 1h-old analysis must not be expired")
		}
	})

	t.Run("maxAge<=0 disables the cap", func(t *testing.T) {
		doc := &codeclarity.Analysis{Created_on: now.Add(-100 * time.Hour)}
		if analysisExpired(doc, now, 0) {
			t.Fatal("cap must be disabled when maxAge<=0")
		}
	})

	t.Run("zero Created_on is never expired", func(t *testing.T) {
		doc := &codeclarity.Analysis{}
		if analysisExpired(doc, now, maxAge) {
			t.Fatal("unknown age must not be retired defensively")
		}
	})
}

func TestStageZeroNeedsRedrive(t *testing.T) {
	const timeout = 30 * time.Minute
	now := time.Now()
	oldCreated := now.Add(-time.Hour)
	freshCreated := now.Add(-time.Minute)
	stale := now.Add(-time.Hour).Format(time.RFC3339Nano)
	recent := now.Add(-time.Minute).Format(time.RFC3339Nano)

	t.Run("pre-download analysis older than timeout needs redrive", func(t *testing.T) {
		doc := &codeclarity.Analysis{
			Stage:      0,
			Created_on: oldCreated,
			Steps:      [][]codeclarity.Step{{step("js-sbom", "")}},
		}
		if !stageZeroNeedsRedrive(doc, now, timeout) {
			t.Fatal("expected redrive for an old pre-download analysis")
		}
	})

	t.Run("freshly submitted pre-download analysis is left alone", func(t *testing.T) {
		doc := &codeclarity.Analysis{
			Stage:      0,
			Created_on: freshCreated,
			Steps:      [][]codeclarity.Step{{step("js-sbom", "")}},
		}
		if stageZeroNeedsRedrive(doc, now, timeout) {
			t.Fatal("fresh submit still downloading must not be re-driven")
		}
	})

	t.Run("stuck STARTED stage-0 step needs redrive", func(t *testing.T) {
		doc := &codeclarity.Analysis{
			Stage:      0,
			Created_on: oldCreated,
			Steps:      [][]codeclarity.Step{{{Name: "js-sbom", Status: codeclarity.STARTED, Started_on: stale}}},
		}
		if !stageZeroNeedsRedrive(doc, now, timeout) {
			t.Fatal("expected redrive for a stuck STARTED stage-0 step")
		}
	})

	t.Run("recently re-driven stage-0 throttles (Started_on recent)", func(t *testing.T) {
		doc := &codeclarity.Analysis{
			Stage:      0,
			Created_on: oldCreated, // old, but a recent re-drive stamped Started_on
			Steps:      [][]codeclarity.Step{{{Name: "js-sbom", Status: "", Started_on: recent}}},
		}
		if stageZeroNeedsRedrive(doc, now, timeout) {
			t.Fatal("a recent re-drive must throttle the next one")
		}
	})

	t.Run("all-success stage 0 never re-drives", func(t *testing.T) {
		doc := &codeclarity.Analysis{
			Stage:      0,
			Created_on: oldCreated,
			Steps:      [][]codeclarity.Step{{step("js-sbom", codeclarity.SUCCESS)}},
		}
		if stageZeroNeedsRedrive(doc, now, timeout) {
			t.Fatal("a completed stage 0 must advance, not re-drive")
		}
	})
}

func TestPlannedSteps(t *testing.T) {
	step := func(names ...string) []codeclarity.Step {
		out := make([]codeclarity.Step, 0, len(names))
		for _, name := range names {
			out = append(out, codeclarity.Step{Name: name})
		}
		return out
	}
	names := func(stages [][]codeclarity.Step) [][]string {
		out := [][]string{}
		for _, stage := range stages {
			row := []string{}
			for _, s := range stage {
				row = append(row, s.Name)
			}
			out = append(out, row)
		}
		return out
	}
	analyzer := [][]codeclarity.Step{step("js-sbom", "php-sbom"), step("vuln-finder", "license-finder")}

	jsOnly := plannedSteps(analyzer, [][]codeclarity.Step{step("js-sbom"), step("vuln-finder", "license-finder")})
	if got, want := names(jsOnly), [][]string{{"js-sbom"}, {"vuln-finder", "license-finder"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("js plan = %v, want %v", got, want)
	}

	if got := plannedSteps(analyzer, nil); len(got) != 2 || len(got[0]) != 2 {
		t.Errorf("no plan should keep every step, got %v", names(got))
	}

	// A stage left empty is dropped, so stage indexes stay valid.
	onlyLater := plannedSteps(analyzer, [][]codeclarity.Step{step("vuln-finder")})
	if got, want := names(onlyLater), [][]string{{"vuln-finder"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("later-stage plan = %v, want %v", got, want)
	}
}

func failedStep(name, ended, reason string) codeclarity.Step {
	s := step(name, codeclarity.FAILURE)
	s.Ended_on = ended
	if reason != "" {
		s.Result = map[string]any{codeclarity.StepFailureReasonKey: reason}
	}
	return s
}

func TestMarkStageFailed(t *testing.T) {
	cases := []struct {
		name  string
		steps [][]codeclarity.Step
		prior string
		want  string
	}{
		{
			name:  "names the plugin and its cause",
			steps: [][]codeclarity.Step{{failedStep("php-sbom", "", "no PHP manifest was found in the project")}},
			want:  "php-sbom failed: no PHP manifest was found in the project",
		},
		{
			name:  "says the plugin failed when it gave no cause",
			steps: [][]codeclarity.Step{{failedStep("vuln-finder", "", "")}},
			want:  "vuln-finder failed unexpectedly. Try again, and contact your administrator if it keeps failing.",
		},
		{
			name: "keeps the failure that finished first",
			steps: [][]codeclarity.Step{{
				failedStep("license-finder", "2026-01-01T10:00:05Z", "later error"),
				failedStep("vuln-finder", "2026-01-01T10:00:01.5Z", "root cause"),
			}},
			want: "vuln-finder failed: root cause",
		},
		{
			name:  "keeps a reason already recorded",
			steps: [][]codeclarity.Step{{failedStep("js-sbom", "", "another cause")}},
			prior: "the repository could not be downloaded",
			want:  "the repository could not be downloaded",
		},
		{
			name:  "hides paths a plugin reported",
			steps: [][]codeclarity.Step{{failedStep("js-sbom", "", "cannot read /private/org/package.json")}},
			want:  "js-sbom failed: cannot read [path]",
		},
		{
			name:  "ignores a skipped plugin",
			steps: [][]codeclarity.Step{{step("js-sbom", codeclarity.SKIPPED), failedStep("php-sbom", "", "bad lock file")}},
			want:  "php-sbom failed: bad lock file",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := &codeclarity.Analysis{Status: codeclarity.ONGOING, Steps: tc.steps, FailureReason: tc.prior}
			markStageFailed(doc)
			if doc.Status != codeclarity.FAILURE {
				t.Errorf("status = %q, want failure", doc.Status)
			}
			if doc.FailureReason != tc.want {
				t.Errorf("reason = %q, want %q", doc.FailureReason, tc.want)
			}
		})
	}
}
