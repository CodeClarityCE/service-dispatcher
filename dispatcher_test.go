package main

import (
	"context"
	"testing"

	"github.com/CodeClarityCE/utility-boilerplates/testdeps"
	"github.com/uptrace/bun"
)

// Same table as the API's plugins migrations create.
const pluginTableDDL = `CREATE TABLE "plugin" ("id" uuid NOT NULL DEFAULT uuid_generate_v4(), "name" character varying, "version" character varying, "description" character varying, "depends_on" jsonb, "config" jsonb, PRIMARY KEY ("id"))`

// pluginRegistry creates the plugin table with one vuln-finder row that depends on js-sbom.
func pluginRegistry(t *testing.T, db *bun.DB) {
	t.Helper()
	for _, stmt := range []string{
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,
		pluginTableDDL,
		`INSERT INTO plugin (name, version, depends_on) VALUES ('vuln-finder', '1.0.0', '["js-sbom"]')`,
	} {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("prepare plugins schema: %v", err)
		}
	}
}

func TestDependencyResolverLoadsPluginDependencies(t *testing.T) {
	db := testdeps.Postgres(t)
	pluginRegistry(t, db)

	resolver := CreateDependencyResolver()
	if err := resolver.LoadPluginConfigurations(db); err != nil {
		t.Fatalf("load plugin configurations: %v", err)
	}

	got := resolver.pluginConfigs["vuln-finder"].DependsOn
	if len(got) != 1 || got[0] != "js-sbom" {
		t.Errorf("vuln-finder depends on %v, want [js-sbom]", got)
	}
}

func TestCreateDispatcherServiceLoadsPluginDependencies(t *testing.T) {
	pluginRegistry(t, testdeps.ServiceDatabases(t))
	testdeps.RabbitMQ(t)

	service, err := CreateDispatcherService()
	if err != nil {
		t.Fatalf("create dispatcher service: %v", err)
	}
	defer service.Close()

	if _, ok := service.dependencyResolver.pluginConfigs["vuln-finder"]; !ok {
		t.Error("want the dispatcher to load vuln-finder from the plugins database")
	}
}
