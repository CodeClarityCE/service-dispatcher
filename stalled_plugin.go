package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	amqp_helper "github.com/CodeClarityCE/utility-amqp-helper"
	"github.com/CodeClarityCE/utility-boilerplates"
	codeclarity "github.com/CodeClarityCE/utility-types/codeclarity_db"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"github.com/uptrace/bun"
)

var stalledLog = &logrus.Logger{
	Out:       logrus.StandardLogger().Out,
	Formatter: &logrus.JSONFormatter{},
	Hooks:     make(logrus.LevelHooks),
	Level:     logrus.InfoLevel,
}

// stalledPluginReason is the step failure reason when its plugin has no consumer.
const stalledPluginReason = "the plugin is not running. Try again shortly, or contact your administrator if it keeps failing."

// A step waiting on a plugin with no consumer is failed after a grace period,
// which lets a restarting plugin reconnect.
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
				stalledLog.WithFields(logrus.Fields{"analysisId": doc.Id.String(), "plugin": st.Name, "error": err.Error()}).
					Warn("Could not count plugin consumers")
				continue
			}
			w.observe(st.Name, consumers, now)
		}
	}
}

// hasStalledStep reports whether any STARTED step of doc waits on a stalled plugin.
func hasStalledStep(doc *codeclarity.Analysis, w *pluginWatch, now time.Time, grace time.Duration) bool {
	for _, stage := range doc.Steps {
		for _, st := range stage {
			if st.Status == codeclarity.STARTED && w.stalled(st.Name, now, grace) {
				return true
			}
		}
	}
	return false
}

// failStalledSteps fails each STARTED step whose plugin has been unconsumed for grace,
// then fails doc the same way as a plugin-reported failure. Returns true if it changed doc.
func failStalledSteps(doc *codeclarity.Analysis, w *pluginWatch, now time.Time, grace time.Duration) bool {
	switch doc.Status {
	case codeclarity.COMPLETED, codeclarity.FAILURE, codeclarity.CANCELLED:
		return false
	}
	failed := false
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
			st.Result[codeclarity.StepFailureReasonKey] = stalledPluginReason
			failed = true
			stalledLog.WithFields(logrus.Fields{"analysisId": doc.Id.String(), "plugin": st.Name, "grace": grace.String()}).
				Warn("Failing analysis step: plugin has no consumer")
		}
	}
	if failed {
		markStageFailed(doc)
	}
	return failed
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
		stalledLog.WithFields(logrus.Fields{"analysisId": id.String(), "error": err.Error()}).
			Error("Could not load analysis for the plugin check")
		return false
	}
	observeStartedPlugins(doc, probe, w, now)
	if !hasStalledStep(doc, w, now, grace) {
		return false
	}
	failed, err := failStalledPluginSteps(id, db, w, now, grace)
	if err != nil {
		stalledLog.WithFields(logrus.Fields{"analysisId": id.String(), "error": err.Error()}).
			Error("Could not fail steps waiting on a stopped plugin")
		return false
	}
	return failed
}

// stalledPluginGrace returns the no-consumer grace period, overridable via STALLED_PLUGIN_GRACE.
func stalledPluginGrace() time.Duration {
	return envDurationSeconds(envStalledPluginGrace, defaultStalledPluginGrace)
}

// memoizeProbe wraps probe so each plugin is probed at most once.
func memoizeProbe(probe consumerProbe) consumerProbe {
	type result struct {
		consumers int
		err       error
	}
	seen := map[string]result{}
	return func(plugin string) (int, error) {
		r, ok := seen[plugin]
		if !ok {
			r.consumers, r.err = probe(plugin)
			seen[plugin] = r
		}
		return r.consumers, r.err
	}
}

// amqpConsumerProbe dials RabbitMQ at most once per reaper pass and counts consumers
// with passive declares. Call close when the pass is done.
type amqpConsumerProbe struct {
	url     string
	dial    func(url string) (*amqp.Connection, error)
	conn    *amqp.Connection
	dialErr error
}

func newAMQPConsumerProbe(service *boilerplates.ServiceBase) *amqpConsumerProbe {
	return &amqpConsumerProbe{url: service.ConfigSvc.AMQP.URL, dial: amqp_helper.Dial}
}

func (p *amqpConsumerProbe) consumers(plugin string) (int, error) {
	if p.conn == nil && p.dialErr == nil {
		p.conn, p.dialErr = p.dial(p.url)
	}
	if p.dialErr != nil {
		return 0, fmt.Errorf("dial rabbitmq: %w", p.dialErr)
	}
	if p.conn.IsClosed() {
		return 0, errors.New("rabbitmq connection closed")
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
