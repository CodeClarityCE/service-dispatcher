package main

import (
	"strings"
	"testing"

	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	plugin_db "github.com/CodeClarityCE/utility-types/plugin_db"
)

var multiLanguageConfig = map[string]codeclarity.LanguagePlugins{
	"javascript": {Plugins: []string{"js-sbom", "vuln-finder", "license-finder"}},
	"php":        {Plugins: []string{"php-sbom", "vuln-finder", "license-finder"}},
}

func multiLanguageSteps() [][]codeclarity.Step {
	return [][]codeclarity.Step{
		{step("js-sbom", ""), step("php-sbom", "")},
		{step("vuln-finder", ""), step("license-finder", "")},
	}
}

func statuses(steps [][]codeclarity.Step) map[string]codeclarity.AnalysisStatus {
	got := map[string]codeclarity.AnalysisStatus{}
	for _, stage := range steps {
		for _, s := range stage {
			got[s.Name] = s.Status
		}
	}
	return got
}

func TestSelectPluginsForLanguages(t *testing.T) {
	cases := []struct {
		name     string
		detected []string
		skipped  []string
		run      []string
	}{
		{"javascript only", []string{"javascript"}, []string{"php-sbom"}, []string{"js-sbom", "vuln-finder", "license-finder"}},
		{"php only", []string{"php"}, []string{"js-sbom"}, []string{"php-sbom", "vuln-finder", "license-finder"}},
		{"mixed", []string{"javascript", "php"}, nil, []string{"js-sbom", "php-sbom", "vuln-finder", "license-finder"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			steps := multiLanguageSteps()
			if reason := selectPluginsForLanguages(steps, multiLanguageConfig, tc.detected); reason != "" {
				t.Fatalf("unexpected failure reason %q", reason)
			}
			got := statuses(steps)
			for _, name := range tc.skipped {
				if got[name] != codeclarity.SKIPPED {
					t.Errorf("%s status = %q, want skipped", name, got[name])
				}
			}
			for _, name := range tc.run {
				if got[name] != "" {
					t.Errorf("%s status = %q, want it left to run", name, got[name])
				}
			}
		})
	}
}

func TestSelectPluginsRecordsWhyAStepWasSkipped(t *testing.T) {
	steps := multiLanguageSteps()
	selectPluginsForLanguages(steps, multiLanguageConfig, []string{"javascript"})
	reason, _ := steps[0][1].Result["skip_reason"].(string)
	if !strings.Contains(reason, "javascript") {
		t.Fatalf("skip reason %q should name the detected languages", reason)
	}
}

func TestSelectPluginsNoSupportedLanguage(t *testing.T) {
	for _, detected := range [][]string{nil, {}, {"cobol"}} {
		steps := multiLanguageSteps()
		reason := selectPluginsForLanguages(steps, multiLanguageConfig, detected)
		if !strings.Contains(reason, "no supported manifest") {
			t.Errorf("detected %v: reason = %q, want it to name the missing manifest", detected, reason)
		}
	}
}

func TestSelectPluginsNewLanguageNeedsOnlyConfig(t *testing.T) {
	config := map[string]codeclarity.LanguagePlugins{
		"javascript": {Plugins: []string{"js-sbom", "vuln-finder"}},
		"python":     {Plugins: []string{"py-sbom", "vuln-finder"}},
	}
	steps := [][]codeclarity.Step{{step("js-sbom", ""), step("py-sbom", "")}, {step("vuln-finder", "")}}
	if reason := selectPluginsForLanguages(steps, config, []string{"python"}); reason != "" {
		t.Fatalf("unexpected failure reason %q", reason)
	}
	got := statuses(steps)
	if got["js-sbom"] != codeclarity.SKIPPED || got["py-sbom"] != "" {
		t.Fatalf("got %v", got)
	}
}

func TestSelectPluginsLeavesUnlistedPluginsRunning(t *testing.T) {
	steps := [][]codeclarity.Step{{step("js-sbom", ""), step("php-sbom", "")}, {step("codeql", "")}}
	selectPluginsForLanguages(steps, multiLanguageConfig, []string{"javascript"})
	if got := statuses(steps)["codeql"]; got != "" {
		t.Fatalf("codeql status = %q, want it left to run", got)
	}
}

func TestSelectPluginsWithoutLanguageConfigRunsEverything(t *testing.T) {
	steps := multiLanguageSteps()
	if reason := selectPluginsForLanguages(steps, nil, nil); reason != "" {
		t.Fatalf("unexpected failure reason %q", reason)
	}
	for name, status := range statuses(steps) {
		if status != "" {
			t.Errorf("%s status = %q, want it left to run", name, status)
		}
	}
}

// A language whose SBOM plugin was left out of the plan would otherwise pass with empty results.
func TestSelectPluginsLanguageMissingFromPlanIsNotSupported(t *testing.T) {
	steps := [][]codeclarity.Step{{step("js-sbom", "")}, {step("vuln-finder", ""), step("license-finder", "")}}
	reason := selectPluginsForLanguages(steps, multiLanguageConfig, []string{"php"})
	if !strings.Contains(reason, "no supported manifest") {
		t.Fatalf("reason = %q, want it to name the missing manifest", reason)
	}
}

func TestSelectPluginsDoesNotTouchStepsAlreadyRun(t *testing.T) {
	steps := [][]codeclarity.Step{{step("js-sbom", codeclarity.SUCCESS), step("php-sbom", codeclarity.STARTED)}}
	selectPluginsForLanguages(steps, multiLanguageConfig, []string{"javascript"})
	if got := statuses(steps); got["php-sbom"] != codeclarity.STARTED {
		t.Fatalf("php-sbom status = %q, want started", got["php-sbom"])
	}
}

func TestSkippedDependencyDoesNotBlockPlugin(t *testing.T) {
	dr := CreateDependencyResolver()
	dr.pluginConfigs["vuln-finder"] = pluginDependingOn("vuln-finder", "js-sbom", "php-sbom")
	analysis := &codeclarity.Analysis{Stage: 1, Steps: [][]codeclarity.Step{
		{step("js-sbom", codeclarity.SKIPPED), step("php-sbom", codeclarity.SUCCESS)},
		{step("vuln-finder", "")},
	}}
	ready, err := dr.GetReadyPlugins(analysis, 1)
	if err != nil || len(ready) != 1 {
		t.Fatalf("ready = %+v, err = %v; want vuln-finder ready", ready, err)
	}
}

func TestUnfinishedDependencyStillBlocksPlugin(t *testing.T) {
	dr := CreateDependencyResolver()
	dr.pluginConfigs["vuln-finder"] = pluginDependingOn("vuln-finder", "js-sbom", "php-sbom")
	analysis := &codeclarity.Analysis{Stage: 1, Steps: [][]codeclarity.Step{
		{step("js-sbom", codeclarity.SKIPPED), step("php-sbom", codeclarity.STARTED)},
		{step("vuln-finder", "")},
	}}
	ready, _ := dr.GetReadyPlugins(analysis, 1)
	if len(ready) != 0 {
		t.Fatalf("ready = %+v, want none", ready)
	}
}

func pluginDependingOn(name string, deps ...string) plugin_db.Plugin {
	return plugin_db.Plugin{Name: name, DependsOn: deps}
}
