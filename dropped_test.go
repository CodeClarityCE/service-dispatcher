package main

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/CodeClarityCE/utility-boilerplates"
	types_amqp "github.com/CodeClarityCE/utility-types/amqp"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// emptyDB holds no rows: every select finds nothing.
type emptyDB struct{}

func (emptyDB) Connect(context.Context) (driver.Conn, error) { return emptyConn{}, nil }
func (emptyDB) Driver() driver.Driver                        { return nil }

type emptyConn struct{}

func (emptyConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (emptyConn) Close() error                        { return nil }
func (emptyConn) Begin() (driver.Tx, error)           { return emptyTx{}, nil }
func (emptyConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return noRows{}, nil
}

type emptyTx struct{}

func (emptyTx) Commit() error   { return nil }
func (emptyTx) Rollback() error { return nil }

type noRows struct{}

func (noRows) Columns() []string         { return []string{"id"} }
func (noRows) Close() error              { return nil }
func (noRows) Next([]driver.Value) error { return io.EOF }

// droppedLogs runs deliver against an empty database and returns what dispatchLog wrote.
func droppedLogs(t *testing.T, deliver func(*boilerplates.ServiceBase)) string {
	t.Helper()
	db := bun.NewDB(sql.OpenDB(emptyDB{}), pgdialect.New())
	defer db.Close()
	var out bytes.Buffer
	previous := dispatchLog.Out
	dispatchLog.Out = &out
	t.Cleanup(func() { dispatchLog.Out = previous })

	deliver(&boilerplates.ServiceBase{DB: &boilerplates.ServiceDatabases{CodeClarity: db}})
	return out.String()
}

func assertMissingAnalysisLine(t *testing.T, logs string, analysisId uuid.UUID) {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(logs), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected one log line, got %q", logs)
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("log line is not JSON: %q", lines[0])
	}
	if entry["level"] != "info" || entry["analysisId"] != analysisId.String() {
		t.Errorf("expected an info line naming %s, got %v", analysisId, entry)
	}
	if msg, _ := entry["msg"].(string); !strings.Contains(msg, "no longer exists") {
		t.Errorf("expected the line to say the analysis no longer exists, got %q", msg)
	}
}

func TestPluginResultForADeletedAnalysisIsDroppedAtInfo(t *testing.T) {
	id := uuid.New()
	body, _ := json.Marshal(types_amqp.PluginDispatcherMessage{AnalysisId: id, Plugin: "vuln-finder"})
	logs := droppedLogs(t, func(s *boilerplates.ServiceBase) {
		dispatchPluginResult(amqp.Delivery{Body: body}, nil, s)
	})
	assertMissingAnalysisLine(t, logs, id)
}

func TestDownloadResultForADeletedAnalysisIsDroppedAtInfo(t *testing.T) {
	id := uuid.New()
	body, _ := json.Marshal(types_amqp.DownloaderDispatcherMessage{AnalysisId: id})
	logs := droppedLogs(t, func(s *boilerplates.ServiceBase) {
		dispatchDownloaderResult(amqp.Delivery{Body: body}, nil, s)
	})
	assertMissingAnalysisLine(t, logs, id)
}

func TestAPIRequestForADeletedAnalysisIsDroppedAtInfo(t *testing.T) {
	id := uuid.New()
	body, _ := json.Marshal(map[string]any{
		"analysis_id":     id.String(),
		"project_id":      uuid.NewString(),
		"organization_id": uuid.NewString(),
	})
	logs := droppedLogs(t, func(s *boilerplates.ServiceBase) {
		dispatchAPIRequest(amqp.Delivery{Body: body}, nil, s)
	})
	assertMissingAnalysisLine(t, logs, id)
}
