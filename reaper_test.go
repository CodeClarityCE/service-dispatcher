package main

import (
	"testing"

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
