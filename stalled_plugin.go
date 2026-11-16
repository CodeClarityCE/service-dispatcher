package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	amqp_helper "github.com/CodeClarityCE/utility-amqp-helper"
	"github.com/CodeClarityCE/utility-boilerplates"
	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/uptrace/bun"
)

// A step waiting on a plugin with no consumer is failed, not left for the 24h
// recovery limit. The grace period lets a plugin that is restarting reconnect.
const (
	defaultStalledPluginGrace = 3 * time.Minute
	envStalledPluginGrace     = "STALLED_PLUGIN_GRACE" // seconds
)

// consumerProbe returns how many consumers are attached to a plugin's dispatch queue.
type consumerProbe func(plugin string) (int, error)

// pluginWatch records since when each plugin has had no consumer. It is kept
// across reaper passes so a plugin must stay unconsumed for the whole grace period.
type pluginWatch struct {
	since map[string]time.Time
	seen  map[string]bool
}

func newPluginWatch() *pluginWatch {
	return &pluginWatch{since: map[string]time.Time{}, seen: map[string]bool{}}
}

// observe records one consumer count for a plugin; any consumer clears its clock.
func (w *pluginWatch) observe(plugin string, consumers int, now time.Time) {
	w.seen[plugin] = true
	if consumers > 0 {
		delete(w.since, plugin)
		return
	}
	if _, ok := w.since[plugin]; !ok {
		w.since[plugin] = now
	}
}

// stalled reports whether a plugin has had no consumer for at least grace.
func (w *pluginWatch) stalled(plugin string, now time.Time, grace time.Duration) bool {
	since, ok := w.since[plugin]
	return ok && now.Sub(since) >= grace
}

// endPass forgets plugins not observed since the last endPass.
func (w *pluginWatch) endPass() {
	for plugin := range w.since {
		if !w.seen[plugin] {
			delete(w.since, plugin)
		}
	}
	w.seen = map[string]bool{}
}

// observeStartedPlugins probes each plugin that has a STARTED step in doc. A failed
// probe records nothing, so an AMQP outage never counts as a missing consumer.
func observeStartedPlugins(doc *codeclarity.Analysis, probe consumerProbe, w *pluginWatch, now time.Time) {
	for _, stage := range doc.Steps {
		for _, st := range stage {
			if st.Status != codeclarity.STARTED {
				continue
			}
			consumers, err := probe(st.Name)
			if err != nil {
				log.Printf("[reaper] count consumers of %s for analysis %s failed: %v", st.Name, doc.Id, err)
				continue
			}
			w.observe(st.Name, consumers, now)
		}
	}
}

// failStalledSteps fails doc when a STARTED step's plugin has been unconsumed for
// grace, naming the plugin in the failure reason. Returns true if it changed doc.
func failStalledSteps(doc *codeclarity.Analysis, w *pluginWatch, now time.Time, grace time.Duration) bool {
	switch doc.Status {
	case codeclarity.COMPLETED, codeclarity.FAILURE, codeclarity.CANCELLED:
		return false
	}
	var plugin string
	for s := range doc.Steps {
		for i := range doc.Steps[s] {
			st := &doc.Steps[s][i]
			if st.Status != codeclarity.STARTED || !w.stalled(st.Name, now, grace) {
				continue
			}
			st.Status = codeclarity.FAILURE
			st.Ended_on = now.Format(time.RFC3339Nano)
			if st.Result == nil {
				st.Result = map[string]any{}
			}
			st.Result[codeclarity.StepFailureReasonKey] = fmt.Sprintf("plugin %s is not running. Try again shortly, or contact your administrator if it keeps failing.", st.Name)
			plugin = st.Name
			log.Printf("[reaper] failing analysis %s: plugin %s has no consumer for %s", doc.Id, st.Name, grace)
		}
	}
	if plugin == "" {
		return false
	}
	doc.Status = codeclarity.FAILURE
	doc.FailureReason = codeclarity.SanitizeFailureReason(fmt.Sprintf("Plugin %s is not running, so this analysis could not finish. Try again shortly, or contact your administrator if it keeps failing.", plugin))
	return true
}

// failStalledPluginSteps locks the analysis, re-checks it and fails it when a step
// is stalled. Returns true if the analysis was failed.
func failStalledPluginSteps(id uuid.UUID, db *bun.DB, w *pluginWatch, now time.Time, grace time.Duration) (bool, error) {
	changed := false
	err := db.RunInTx(context.Background(), &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		doc := &codeclarity.Analysis{Id: id}
		if e := tx.NewSelect().Model(doc).WherePK().For("UPDATE").Scan(ctx); e != nil {
			return fmt.Errorf("lock+reload analysis %s: %w", id, e)
		}
		if !failStalledSteps(doc, w, now, grace) {
			return nil
		}
		changed = true
		_, e := tx.NewUpdate().Model(doc).WherePK().Exec(ctx)
		return e
	})
	return changed, err
}

// checkStalledPlugins observes the plugins an analysis waits on and fails it when one
// has had no consumer for grace. Returns true if the analysis was failed.
func checkStalledPlugins(id uuid.UUID, db *bun.DB, probe consumerProbe, w *pluginWatch, now time.Time, grace time.Duration) bool {
	doc := &codeclarity.Analysis{Id: id}
	if err := db.NewSelect().Model(doc).WherePK().Scan(context.Background()); err != nil {
		log.Printf("[reaper] load %s for plugin check failed: %v", id, err)
		return false
	}
	observeStartedPlugins(doc, probe, w, now)
	failed, err := failStalledPluginSteps(id, db, w, now, grace)
	if err != nil {
		log.Printf("[reaper] fail stalled steps of %s failed: %v", id, err)
		return false
	}
	return failed
}

// stalledPluginGrace returns the no-consumer grace period, overridable via STALLED_PLUGIN_GRACE.
func stalledPluginGrace() time.Duration {
	return envDurationSeconds(envStalledPluginGrace, defaultStalledPluginGrace)
}

// amqpConsumerProbe opens one connection for a reaper pass and counts consumers with
// passive declares. Call close when the pass is done.
type amqpConsumerProbe struct {
	url  string
	conn *amqp.Connection
}

func newAMQPConsumerProbe(service *boilerplates.ServiceBase) *amqpConsumerProbe {
	return &amqpConsumerProbe{url: service.ConfigSvc.AMQP.URL}
}

func (p *amqpConsumerProbe) consumers(plugin string) (int, error) {
	if p.conn == nil || p.conn.IsClosed() {
		conn, err := amqp_helper.Dial(p.url)
		if err != nil {
			return 0, fmt.Errorf("dial rabbitmq: %w", err)
		}
		p.conn = conn
	}
	ch, err := p.conn.Channel()
	if err != nil {
		return 0, fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive("dispatcher_"+plugin, true, false, false, false, nil)
	if err != nil {
		var amqpErr *amqp.Error
		if errors.As(err, &amqpErr) && amqpErr.Code == amqp.NotFound {
			return 0, nil
		}
		return 0, fmt.Errorf("passive declare of dispatcher_%s: %w", plugin, err)
	}
	return q.Consumers, nil
}

func (p *amqpConsumerProbe) close() {
	if p.conn != nil && !p.conn.IsClosed() {
		_ = p.conn.Close()
	}
}
