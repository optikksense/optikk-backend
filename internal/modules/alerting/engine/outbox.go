package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/channels"
)

// OutboxPayload is the JSON blob persisted per transition. It carries enough
// context for the relay to render + deliver the notification without touching
// any other tables.
type OutboxPayload struct {
	RuleID          int64             `json:"rule_id"`
	TeamID          int64             `json:"team_id"`
	RuleName        string            `json:"rule_name"`
	SlackWebhookURL string            `json:"slack_webhook_url"`
	ToState         string            `json:"to_state"`
	Values          map[string]any    `json:"values"`
	DeployHint      string            `json:"deploy_hint"`
	DeepLink        string            `json:"deep_link"`
	Tags            map[string]string `json:"tags"`
}

// OutboxStore persists notification transitions in MySQL so that a pod crash
// between the in-memory Dispatcher fast-path and the Slack POST does not drop
// the notification.
type OutboxStore struct {
	db *sql.DB
}

func NewOutboxStore(db *sql.DB) *OutboxStore { return &OutboxStore{db: db} }

// Enqueue inserts a pending outbox row. The (alert_id, instance_key, transition_seq)
// unique key collapses duplicate writes (evaluator retries on the same
// transition) into a single row via INSERT IGNORE.
func (s *OutboxStore) Enqueue(ctx context.Context, teamID, alertID int64, instanceKey string, transitionSeq int64, payload OutboxPayload) error {
	if s == nil || s.db == nil {
		return nil
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT IGNORE INTO observability.alert_outbox
			(team_id, alert_id, instance_key, transition_seq, payload_json)
		 VALUES (?, ?, ?, ?, ?)`,
		teamID, alertID, instanceKey, transitionSeq, string(buf))
	return err
}

type outboxRow struct {
	ID            int64
	TeamID        int64
	AlertID       int64
	InstanceKey   string
	TransitionSeq int64
	Payload       OutboxPayload
	Attempts      int
}

// Claim returns up to limit rows that are undelivered and ripe for retry.
// SELECT ... FOR UPDATE SKIP LOCKED lets multiple relay goroutines on
// different pods process non-overlapping rows concurrently without blocking.
func (s *OutboxStore) Claim(ctx context.Context, limit int) ([]outboxRow, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }() //nolint:errcheck // committed on success

	rows, err := tx.QueryContext(ctx,
		`SELECT id, team_id, alert_id, instance_key, transition_seq, payload_json, attempts
		   FROM observability.alert_outbox
		  WHERE delivered_at IS NULL AND next_attempt_at <= UTC_TIMESTAMP()
		  ORDER BY id ASC
		  LIMIT ?
		  FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // ro cursor

	var out []outboxRow
	var ids []int64
	for rows.Next() {
		var r outboxRow
		var payload string
		if err := rows.Scan(&r.ID, &r.TeamID, &r.AlertID, &r.InstanceKey, &r.TransitionSeq, &payload, &r.Attempts); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(payload), &r.Payload); err != nil {
			return nil, err
		}
		out = append(out, r)
		ids = append(ids, r.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(ids) > 0 {
		if _, err := tx.ExecContext(ctx,
			`UPDATE observability.alert_outbox
			    SET next_attempt_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL 60 SECOND)
			  WHERE id IN (`+placeholders(len(ids))+`)`,
			int64SliceArgs(ids)...); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *OutboxStore) MarkDelivered(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE observability.alert_outbox
		    SET delivered_at = UTC_TIMESTAMP()
		  WHERE id = ? AND delivered_at IS NULL`, id)
	return err
}

// MarkDeliveredByKey is called by the Dispatcher fast-path on successful
// Slack send; collapses the race with the relay so exactly one path flips
// delivered_at from NULL.
func (s *OutboxStore) MarkDeliveredByKey(ctx context.Context, alertID int64, instanceKey string, transitionSeq int64) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE observability.alert_outbox
		    SET delivered_at = UTC_TIMESTAMP()
		  WHERE alert_id = ? AND instance_key = ? AND transition_seq = ? AND delivered_at IS NULL`,
		alertID, instanceKey, transitionSeq)
	return err
}

func (s *OutboxStore) MarkFailed(ctx context.Context, id int64, attempts int, errMsg string) error {
	// Exponential backoff: 30s, 2m, 8m, 32m, 128m, capped at 2h.
	backoff := 30 * time.Second
	for i := 0; i < attempts && i < 7; i++ {
		backoff *= 4
	}
	if backoff > 2*time.Hour {
		backoff = 2 * time.Hour
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE observability.alert_outbox
		    SET attempts = ?,
		        last_error = ?,
		        next_attempt_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND)
		  WHERE id = ?`,
		attempts, truncate(errMsg, 1024), int(backoff.Seconds()), id)
	return err
}

// OutboxRelay polls the outbox table on a fixed cadence and delivers any row
// still undelivered. Durable fallback for the Dispatcher fast-path.
type OutboxRelay struct {
	store    *OutboxStore
	slack    channels.Channel
	interval time.Duration
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewOutboxRelay(store *OutboxStore) *OutboxRelay {
	return &OutboxRelay{
		store:    store,
		slack:    channels.NewSlack(),
		interval: 15 * time.Second,
	}
}

func (r *OutboxRelay) Start() {
	if r == nil || r.store == nil || r.store.db == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)
	go r.run(ctx)
}

func (r *OutboxRelay) Stop() error {
	if r == nil || r.cancel == nil {
		return nil
	}
	r.cancel()
	r.wg.Wait()
	return nil
}

func (r *OutboxRelay) run(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.drain(ctx)
		}
	}
}

func (r *OutboxRelay) drain(ctx context.Context) {
	rows, err := r.store.Claim(ctx, 100)
	if err != nil {
		slog.Debug("alerting outbox: claim failed", slog.Any("error", err))
		return
	}
	for _, row := range rows {
		if row.Payload.SlackWebhookURL == "" {
			_ = r.store.MarkDelivered(ctx, row.ID) //nolint:errcheck // best-effort
			continue
		}
		rendered := channels.Rendered{
			Title:       row.Payload.RuleName,
			Body:        renderOutboxBody(row.Payload),
			Severity:    row.Payload.ToState,
			DeepLinkURL: row.Payload.DeepLink,
			Tags:        row.Payload.Tags,
		}
		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := r.slack.Send(sendCtx, row.Payload.SlackWebhookURL, rendered)
		cancel()
		if err != nil {
			if markErr := r.store.MarkFailed(ctx, row.ID, row.Attempts+1, err.Error()); markErr != nil {
				slog.Debug("alerting outbox: mark-failed failed", slog.Any("error", markErr))
			}
			continue
		}
		if err := r.store.MarkDelivered(ctx, row.ID); err != nil {
			slog.Debug("alerting outbox: mark-delivered failed", slog.Any("error", err))
		}
	}
}

func renderOutboxBody(p OutboxPayload) string {
	lines := []string{"Alert " + p.RuleName + " → " + p.ToState}
	if len(p.Values) > 0 {
		keys := make([]string, 0, len(p.Values))
		for k := range p.Values {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			buf, _ := json.Marshal(p.Values[k]) //nolint:errcheck // marshal of scalar always succeeds
			lines = append(lines, k+": "+string(buf))
		}
	}
	if p.DeployHint != "" {
		lines = append(lines, "Recent deploy: "+p.DeployHint)
	}
	if p.DeepLink != "" {
		lines = append(lines, "Open in Optik: "+p.DeepLink)
	}
	return strings.Join(lines, "\n")
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, 0, n*2)
	for i := 0; i < n; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '?')
	}
	return string(b)
}

func int64SliceArgs(ids []int64) []any {
	out := make([]any, len(ids))
	for i, v := range ids {
		out[i] = v
	}
	return out
}
