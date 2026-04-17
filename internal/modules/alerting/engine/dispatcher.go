package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/channels"
)

// dispatchItem is the in-memory work item enqueued by the evaluator when it
// detects a transition.
type dispatchItem struct {
	Rule       *shared.Rule
	Instance   *shared.Instance
	Transition shared.Transition
	Now        time.Time
	DeployRefs []shared.DeployRef
}

// Dispatcher consumes transitions and sends them through registered channels
// with idempotency + per-rule rate limiting. Survives pod crash via the
// outbox store (see OutboxRelay).
type Dispatcher struct {
	slack    channels.Channel
	events   EventStore
	outbox   *OutboxStore
	ch       chan dispatchItem
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	baseURL  string
	seenMu   sync.Mutex
	seenKeys map[string]time.Time
}

func NewDispatcher(events EventStore, baseURL string) *Dispatcher {
	return &Dispatcher{
		slack:    channels.NewSlack(),
		events:   events,
		ch:       make(chan dispatchItem, 512),
		baseURL:  baseURL,
		seenKeys: make(map[string]time.Time),
	}
}

// SetOutbox attaches the durable outbox store. Wiring is decoupled from the
// constructor so tests can spin a Dispatcher without MySQL.
func (d *Dispatcher) SetOutbox(s *OutboxStore) { d.outbox = s }

// SendSlack is used by the slack submodule's TestSlack endpoint.
func (d *Dispatcher) SendSlack(ctx context.Context, webhookURL string, rendered channels.Rendered) error {
	return d.slack.Send(ctx, webhookURL, rendered)
}

func (d *Dispatcher) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.wg.Add(1)
	go d.run(ctx)
}

func (d *Dispatcher) Stop() error {
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
	return nil
}

// Enqueue is called by the evaluator loop on each detected transition.
func (d *Dispatcher) Enqueue(rule *shared.Rule, instance *shared.Instance, transition shared.Transition, now time.Time, deployRefs []shared.DeployRef) {
	item := dispatchItem{
		Rule: rule, Instance: instance, Transition: transition, Now: now, DeployRefs: deployRefs,
	}
	select {
	case d.ch <- item:
	default:
		slog.Warn("alerting: dispatch buffer full, dropping item",
			slog.Int64("alert_id", rule.ID),
			slog.String("instance_key", instance.InstanceKey))
	}
}

const seenKeyTTL = 1 * time.Hour

func (d *Dispatcher) run(ctx context.Context) {
	defer d.wg.Done()
	evictTicker := time.NewTicker(10 * time.Minute)
	defer evictTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-evictTicker.C:
			d.evictExpiredKeys()
		case item, ok := <-d.ch:
			if !ok {
				return
			}
			d.handle(ctx, item)
		}
	}
}

func (d *Dispatcher) evictExpiredKeys() {
	d.seenMu.Lock()
	defer d.seenMu.Unlock()
	cutoff := time.Now().Add(-seenKeyTTL)
	for key, ts := range d.seenKeys {
		if ts.Before(cutoff) {
			delete(d.seenKeys, key)
		}
	}
}

func (d *Dispatcher) handle(ctx context.Context, item dispatchItem) {
	idem := idempotencyKey(item.Rule.ID, item.Instance.InstanceKey, item.Instance.LastTransitionSeq)
	d.seenMu.Lock()
	if _, ok := d.seenKeys[idem]; ok {
		d.seenMu.Unlock()
		return
	}
	d.seenKeys[idem] = time.Now()
	d.seenMu.Unlock()

	deployHint := ""
	if len(item.DeployRefs) > 0 {
		ref := item.DeployRefs[len(item.DeployRefs)-1]
		deployHint = ref.ServiceName + " " + ref.Version
	}
	deepLink := d.baseURL + "/alerts/rules/" + shared.Itoa(item.Rule.ID)
	r := shared.RenderRuleNotification(
		item.Rule,
		shared.RuleDefinitionFromRow(item.Rule),
		item.Transition.ToState,
		item.Instance.Values,
		deployHint,
		deepLink,
	)
	r.Tags = item.Instance.GroupValues
	if item.Rule.SlackWebhookURL != "" {
		if err := d.slack.Send(ctx, item.Rule.SlackWebhookURL, r); err != nil {
			slog.Error("alerting: slack send failed", slog.Any("error", err))
			if d.events != nil {
				if writeErr := d.events.WriteEvent(ctx, shared.AlertEvent{
					TeamID:  uint32(item.Rule.TeamID), //nolint:gosec
					AlertID: item.Rule.ID, InstanceKey: item.Instance.InstanceKey,
					Kind: shared.EventKindDispatchFailed, Message: err.Error(),
				}); writeErr != nil {
					slog.Debug("alerting: write audit event failed", slog.Any("error", writeErr))
				}
			}
			return
		}
	}
	// Fast-path succeeded — collapse the outbox row so the relay does not
	// double-deliver. Race-safe via delivered_at IS NULL guard on both sides.
	if d.outbox != nil {
		if err := d.outbox.MarkDeliveredByKey(ctx, item.Rule.ID, item.Instance.InstanceKey, item.Instance.LastTransitionSeq); err != nil {
			slog.Debug("alerting: mark outbox delivered failed", slog.Any("error", err))
		}
	}
	deployJSON, _ := json.Marshal(item.DeployRefs) //nolint:errcheck // marshal of known-safe struct slice
	if d.events != nil {
		if err := d.events.WriteEvent(ctx, shared.AlertEvent{
			TeamID:       uint32(item.Rule.TeamID), //nolint:gosec
			AlertID:      item.Rule.ID,
			InstanceKey:  item.Instance.InstanceKey,
			Kind:         shared.EventKindDispatch,
			FromState:    item.Transition.FromState,
			ToState:      item.Transition.ToState,
			DeployRefs:   string(deployJSON),
			TransitionID: item.Instance.LastTransitionSeq,
		}); err != nil {
			slog.Debug("alerting: write audit event failed", slog.Any("error", err))
		}
	}
	now := time.Now().UTC()
	item.Instance.LastNotifiedAt = &now
	item.Instance.LastNotifiedSeq = item.Instance.LastTransitionSeq
}

func idempotencyKey(alertID int64, instanceKey string, seq int64) string {
	return shared.Itoa(alertID) + ":" + instanceKey + ":" + shared.Itoa(seq)
}
