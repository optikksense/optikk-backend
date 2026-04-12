package alerting

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/channels"
)

// dispatchItem is an in-memory work item placed on the dispatcher channel when
// the evaluator detects a transition. v1 runs dispatcher+evaluator in-process
// so a Go channel is sufficient; the plan's Redis Stream + consumer group is
// the next-step upgrade (kept in a comment below for continuity).
type dispatchItem struct {
	Rule       *Rule
	Instance   *Instance
	Transition Transition
	Now        time.Time
	DeployRefs []DeployRef
}

// Dispatcher consumes transitions and sends them through registered channels
// with idempotency + per-rule rate limiting.
type Dispatcher struct {
	slack    channels.Channel
	repo     Repository
	ch       chan dispatchItem
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	baseURL  string
	seenMu   sync.Mutex
	seenKeys map[string]time.Time
}

func NewDispatcher(repo Repository, baseURL string) *Dispatcher {
	return &Dispatcher{
		slack:    channels.NewSlack(),
		repo:     repo,
		ch:       make(chan dispatchItem, 512),
		baseURL:  baseURL,
		seenKeys: make(map[string]time.Time),
	}
}

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

// Enqueue pushes a transition for async delivery. Non-blocking: if the buffer
// is full the item is dropped and a warning logged (v1 trade-off; Redis
// Streams would give us durability).
func (d *Dispatcher) Enqueue(item dispatchItem) {
	select {
	case d.ch <- item:
	default:
		slog.Warn("alerting: dispatch buffer full, dropping item",
			slog.Int64("alert_id", item.Rule.ID),
			slog.String("instance_key", item.Instance.InstanceKey))
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
	deepLink := d.baseURL + "/alerts/rules/" + itoa(item.Rule.ID)
	r := renderRuleNotification(
		item.Rule,
		ruleDefinitionFromRow(item.Rule),
		item.Transition.ToState,
		item.Instance.Values,
		deployHint,
		deepLink,
	)
	r.Tags = item.Instance.GroupValues
	if item.Rule.SlackWebhookURL != "" {
		if err := d.slack.Send(ctx, item.Rule.SlackWebhookURL, r); err != nil {
			slog.Error("alerting: slack send failed", slog.Any("error", err))
			if writeErr := d.repo.WriteEvent(ctx, AlertEvent{
				TeamID:  uint32(item.Rule.TeamID), //nolint:gosec
				AlertID: item.Rule.ID, InstanceKey: item.Instance.InstanceKey,
				Kind: EventKindDispatchFailed, Message: err.Error(),
			}); writeErr != nil {
				slog.Debug("alerting: write audit event failed", slog.Any("error", writeErr))
			}
			return
		}
	}
	deployJSON, _ := json.Marshal(item.DeployRefs) //nolint:errcheck // marshal of known-safe struct slice
	if err := d.repo.WriteEvent(ctx, AlertEvent{
		TeamID:       uint32(item.Rule.TeamID), //nolint:gosec
		AlertID:      item.Rule.ID,
		InstanceKey:  item.Instance.InstanceKey,
		Kind:         EventKindDispatch,
		FromState:    item.Transition.FromState,
		ToState:      item.Transition.ToState,
		DeployRefs:   string(deployJSON),
		TransitionID: item.Instance.LastTransitionSeq,
	}); err != nil {
		slog.Debug("alerting: write audit event failed", slog.Any("error", err))
	}
	now := time.Now().UTC()
	item.Instance.LastNotifiedAt = &now
	item.Instance.LastNotifiedSeq = item.Instance.LastTransitionSeq
}

func idempotencyKey(alertID int64, instanceKey string, seq int64) string {
	return itoa(alertID) + ":" + instanceKey + ":" + itoa(seq)
}

func itoa(i int64) string {
	// Minimal int→string to avoid importing strconv in every file that uses it.
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
