package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

const testGrace = 3 * time.Minute

func TestPluginWatch(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("no consumer is stalled only after the grace period", func(t *testing.T) {
		w := newPluginWatch()
		w.observe("vuln-finder", 0, t0)
		if w.stalled("vuln-finder", t0.Add(testGrace-time.Second), testGrace) {
			t.Fatal("stalled before the grace period ended")
		}
		if !w.stalled("vuln-finder", t0.Add(testGrace), testGrace) {
			t.Fatal("not stalled after the grace period with no consumer")
		}
	})

	t.Run("first sighting after a restart starts the clock", func(t *testing.T) {
		w := newPluginWatch()
		restart := t0.Add(24 * time.Hour)
		w.observe("js-sbom", 0, restart)
		if w.stalled("js-sbom", restart.Add(time.Minute), testGrace) {
			t.Fatal("a plugin still connecting after restart was stalled")
		}
	})

	t.Run("a consumer resets the clock", func(t *testing.T) {
		w := newPluginWatch()
		w.observe("vuln-finder", 0, t0)
		w.observe("vuln-finder", 1, t0.Add(testGrace/2))
		w.observe("vuln-finder", 0, t0.Add(testGrace))
		if w.stalled("vuln-finder", t0.Add(testGrace+time.Second), testGrace) {
			t.Fatal("clock was not reset by a consumer")
		}
	})

	t.Run("a slow plugin that keeps a consumer is never stalled", func(t *testing.T) {
		w := newPluginWatch()
		for i := range 20 {
			w.observe("license-finder", 1, t0.Add(time.Duration(i)*time.Minute))
		}
		if w.stalled("license-finder", t0.Add(time.Hour), testGrace) {
			t.Fatal("a plugin with a consumer was stalled")
		}
	})

	t.Run("a plugin not observed in a pass is forgotten", func(t *testing.T) {
		w := newPluginWatch()
		w.observe("php-sbom", 0, t0)
		w.endPass()
		w.endPass()
		later := t0.Add(10 * testGrace)
		w.observe("php-sbom", 0, later)
		if w.stalled("php-sbom", later.Add(time.Second), testGrace) {
			t.Fatal("an old observation survived a pass without the plugin")
		}
	})
}

func TestFailStalledSteps(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := t0.Add(testGrace + time.Minute)

	newDoc := func(stage1 ...codeclarity.Step) *codeclarity.Analysis {
		return &codeclarity.Analysis{
			Id:     uuid.New(),
			Status: codeclarity.STARTED,
			Stage:  1,
			Steps: [][]codeclarity.Step{
				{step("js-sbom", codeclarity.SUCCESS)},
				stage1,
			},
		}
	}

	t.Run("a step on a plugin with no consumer fails the analysis and names the plugin", func(t *testing.T) {
		w := newPluginWatch()
		w.observe("vuln-finder", 0, t0)
		doc := newDoc(startedStep("vuln-finder", t0))

		if !failStalledSteps(doc, w, now, testGrace) {
			t.Fatal("expected the stalled step to fail the analysis")
		}
		if doc.Status != codeclarity.FAILURE {
			t.Fatalf("analysis status = %s, want failure", doc.Status)
		}
		if !strings.Contains(doc.FailureReason, "vuln-finder") {
			t.Fatalf("failure reason %q does not name the plugin", doc.FailureReason)
		}
		if want := stageFailureReason(doc.Steps); doc.FailureReason != want {
			t.Fatalf("failure reason %q, want the normal step failure reason %q", doc.FailureReason, want)
		}
		if got := doc.Steps[1][0].Status; got != codeclarity.FAILURE {
			t.Fatalf("step status = %s, want failure", got)
		}
	})

	t.Run("a step whose plugin is still consumed is left alone", func(t *testing.T) {
		w := newPluginWatch()
		w.observe("vuln-finder", 1, t0)
		doc := newDoc(startedStep("vuln-finder", t0))

		if failStalledSteps(doc, w, now, testGrace) {
			t.Fatal("a plugin with a consumer must not fail the analysis")
		}
		if doc.Status != codeclarity.STARTED || doc.Steps[1][0].Status != codeclarity.STARTED {
			t.Fatalf("analysis or step changed: status=%s step=%s", doc.Status, doc.Steps[1][0].Status)
		}
	})

	t.Run("a plugin inside the grace period is left alone", func(t *testing.T) {
		w := newPluginWatch()
		w.observe("vuln-finder", 0, now.Add(-time.Minute))
		doc := newDoc(startedStep("vuln-finder", t0))

		if failStalledSteps(doc, w, now, testGrace) {
			t.Fatal("failed a plugin still inside the grace period")
		}
	})

	t.Run("a plugin never observed is left alone", func(t *testing.T) {
		doc := newDoc(startedStep("vuln-finder", t0))
		if failStalledSteps(doc, newPluginWatch(), now, testGrace) {
			t.Fatal("failed a step whose plugin was never probed")
		}
	})
}

func TestHasStalledStep(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	doc := &codeclarity.Analysis{Steps: [][]codeclarity.Step{{startedStep("vuln-finder", t0)}}}

	w := newPluginWatch()
	w.observe("vuln-finder", 0, t0)
	if hasStalledStep(doc, w, t0.Add(time.Minute), testGrace) {
		t.Fatal("reported a stalled step inside the grace period")
	}
	if !hasStalledStep(doc, w, t0.Add(testGrace), testGrace) {
		t.Fatal("missed a step whose plugin is past the grace period")
	}
}

func TestMemoizeProbe(t *testing.T) {
	calls := map[string]int{}
	probe := memoizeProbe(func(plugin string) (int, error) {
		calls[plugin]++
		return 0, errors.New("amqp down")
	})
	for range 3 {
		_, _ = probe("vuln-finder")
		_, _ = probe("js-sbom")
	}
	if calls["vuln-finder"] != 1 || calls["js-sbom"] != 1 {
		t.Fatalf("probe calls = %v, want one per plugin", calls)
	}
}

func TestAMQPConsumerProbeDialsOnce(t *testing.T) {
	dials := 0
	p := &amqpConsumerProbe{dial: func(string) (*amqp.Connection, error) {
		dials++
		return nil, errors.New("connection refused")
	}}
	for _, plugin := range []string{"vuln-finder", "js-sbom", "license-finder"} {
		if _, err := p.consumers(plugin); err == nil {
			t.Fatalf("probe of %s succeeded with no broker", plugin)
		}
	}
	if dials != 1 {
		t.Fatalf("dialed %d times in one pass, want 1", dials)
	}
}

func TestObserveStartedPlugins(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	doc := &codeclarity.Analysis{
		Id: uuid.New(),
		Steps: [][]codeclarity.Step{
			{step("js-sbom", codeclarity.SUCCESS)},
			{startedStep("vuln-finder", t0), step("license-finder", codeclarity.SUCCESS)},
		},
	}

	t.Run("probes only plugins with a STARTED step", func(t *testing.T) {
		var probed []string
		probe := func(plugin string) (int, error) {
			probed = append(probed, plugin)
			return 0, nil
		}
		observeStartedPlugins(doc, probe, newPluginWatch(), t0)
		if len(probed) != 1 || probed[0] != "vuln-finder" {
			t.Fatalf("probed %v, want [vuln-finder]", probed)
		}
	})

	t.Run("a failed probe records nothing, so no step is failed on a broken AMQP path", func(t *testing.T) {
		w := newPluginWatch()
		probe := func(string) (int, error) { return 0, errors.New("amqp down") }
		observeStartedPlugins(doc, probe, w, t0)
		if w.stalled("vuln-finder", t0.Add(24*time.Hour), testGrace) {
			t.Fatal("a failed probe counted as a missing consumer")
		}
	})
}

// startedStep is a STARTED step dispatched at the given time.
func startedStep(name string, started time.Time) codeclarity.Step {
	st := step(name, codeclarity.STARTED)
	st.Started_on = started.Format(time.RFC3339Nano)
	return st
}
