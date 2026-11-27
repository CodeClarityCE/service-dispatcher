package main

import (
	"time"

	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
)

// packageImportDoneKey is the Config key the package follower fills, per language, once
// it has imported an SBOM's packages into the knowledge database.
const packageImportDoneKey = "package_imports_done"

// packageImportDone reports whether the follower has imported the packages of an SBOM
// step's language. Steps that are not SBOMs have no import to wait for.
func packageImportDone(doc *codeclarity.Analysis, stepName string) bool {
	language, isSbom := sbomLanguages[stepName]
	if !isSbom {
		return true
	}
	ready, _ := doc.Config[packageImportDoneKey].(map[string]any)
	_, done := ready[language]
	return done
}

// markPackageImportDone records that a language's packages need no further import.
func markPackageImportDone(doc *codeclarity.Analysis, language string, now time.Time) {
	if doc.Config == nil {
		doc.Config = map[string]any{}
	}
	ready, _ := doc.Config[packageImportDoneKey].(map[string]any)
	if ready == nil {
		ready = map[string]any{}
	}
	ready[language] = now.Format(time.RFC3339Nano)
	doc.Config[packageImportDoneKey] = ready
}

// awaitingPackageImport reports whether the analysis waits on the package follower:
// an import is running, or a finished SBOM's packages are not imported yet.
func awaitingPackageImport(doc *codeclarity.Analysis) bool {
	return doc.Status == codeclarity.UPDATING_DB || len(pendingPackageImports(doc)) > 0
}

// pendingPackageImports lists the languages whose SBOM succeeded but whose packages the
// follower has not imported yet.
func pendingPackageImports(doc *codeclarity.Analysis) []string {
	var pending []string
	for _, stage := range doc.Steps {
		for _, st := range stage {
			if st.Status == codeclarity.SUCCESS && !packageImportDone(doc, st.Name) {
				pending = append(pending, sbomLanguages[st.Name])
			}
		}
	}
	return pending
}
