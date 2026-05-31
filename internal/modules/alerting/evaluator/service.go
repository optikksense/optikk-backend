package evaluator

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/dispatch"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/expr"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/query"
	tmpl "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/template"
)

// Service runs the per-tick evaluation loop. It's stateless except for the
// injected dependencies; one Service instance handles all monitors.
type Service struct {
	repo       Repository
	queries    query.Registry
	dispatcher dispatch.Dispatcher
	concurrency int
}

func NewService(repo Repository, queries query.Registry, dispatcher dispatch.Dispatcher) *Service {
	return &Service{
		repo:        repo,
		queries:     queries,
		dispatcher:  dispatcher,
		concurrency: 16,
	}
}

// Tick processes one round of due monitors. Bounded-concurrency fan-out via
// a buffered semaphore so the run.Group can shut down cleanly via ctx.
func (s *Service) Tick(ctx context.Context, now time.Time) error {
	due, err := s.repo.LoadDue(ctx, now, 500)
	if err != nil {
		return err
	}
	if len(due) == 0 {
		return nil
	}
	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	for _, m := range due {
		m := m
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.evalOne(ctx, m, now)
		}()
	}
	wg.Wait()
	return nil
}

// evalOne handles a single monitor: query -> decide -> persist -> dispatch.
// Errors are logged but never bubble up — one bad monitor can't kill the tick.
func (s *Service) evalOne(ctx context.Context, due DueMonitor, now time.Time) {
	m := due.Monitor
	state := due.State
	cond := query.DecodeConditions(m)
	q := query.DecodeQuery(m)
	scope := query.DecodeScope(m)

	backend, err := s.queries.For(m.Type)
	if err != nil {
		slog.WarnContext(ctx, "alerting: no query backend for type", slog.String("type", m.Type), slog.Int64("monitor_id", m.ID))
		return
	}
	res, err := backend.Scalar(ctx, m, q, scope, cond, now)
	if err != nil {
		slog.WarnContext(ctx, "alerting: scalar eval failed", slog.Int64("monitor_id", m.ID), slog.Any("error", err))
		// still bump next_evaluation_at so we don't spin
		_ = s.repo.UpdateState(ctx, nextEvalOnly(m, state, now))
		return
	}

	renotify := int64(0)
	if m.RenotifyEverySec.Valid {
		renotify = m.RenotifyEverySec.Int64
	}
	decision := expr.Decide(state, m, cond, res.Value, res.HasData, renotify, now)

	args := buildUpdateArgs(m, state, decision, res, now)
	if err := s.repo.UpdateState(ctx, args); err != nil {
		slog.WarnContext(ctx, "alerting: update state failed", slog.Int64("monitor_id", m.ID), slog.Any("error", err))
		return
	}

	if decision.Transition && (decision.NewStatus == "alert" || decision.NewStatus == "warn") {
		_ = s.repo.InsertEvent(ctx, models.MonitorEventRow{
			MonitorID: m.ID, TeamID: m.TeamID, Kind: "triggered",
			Value:     sql.NullFloat64{Valid: true, Float64: res.Value},
			Threshold: thresholdForCond(cond),
			StartedAt: now,
		})
	}
	if decision.IsRecovery {
		_ = s.repo.InsertEvent(ctx, models.MonitorEventRow{
			MonitorID: m.ID, TeamID: m.TeamID, Kind: "recovered",
			Value:     sql.NullFloat64{Valid: true, Float64: res.Value},
			Threshold: thresholdForCond(cond),
			StartedAt: now,
		})
	}

	if decision.ShouldNotify && !isMuted(m, now) {
		s.dispatchAll(ctx, m, cond, res, decision, now)
	}
}

// nextEvalOnly returns args that only advance the next_evaluation_at clock
// without changing status — used when the underlying query errored.
func nextEvalOnly(m models.MonitorRow, state models.MonitorStateRow, now time.Time) UpdateStateArgs {
	status := state.Status
	if status == "" {
		status = "no_data"
	}
	return UpdateStateArgs{
		MonitorID:          m.ID,
		PrevStatus:         status,
		NewStatus:          status,
		LastEvaluatedAt:    now,
		NextEvaluationAt:   now.Add(time.Duration(m.EvalEverySec) * time.Second),
		IncrementEvalCount: true,
	}
}

func buildUpdateArgs(m models.MonitorRow, state models.MonitorStateRow, d expr.Decision, res query.ScalarResult, now time.Time) UpdateStateArgs {
	prev := state.Status
	if prev == "" {
		prev = "no_data"
	}
	args := UpdateStateArgs{
		MonitorID:          m.ID,
		PrevStatus:         prev,
		NewStatus:          d.NewStatus,
		LastEvaluatedAt:    now,
		NextEvaluationAt:   now.Add(time.Duration(m.EvalEverySec) * time.Second),
		IncrementEvalCount: true,
	}
	if res.HasData {
		args.CurrentValue = sql.NullFloat64{Valid: true, Float64: res.Value}
	}
	if d.NewStatus == "alert" || d.NewStatus == "warn" {
		if state.TriggeredAt.Valid {
			args.TriggeredAt = state.TriggeredAt
		} else {
			args.TriggeredAt = sql.NullTime{Valid: true, Time: now}
		}
	}
	if d.ShouldNotify {
		args.LastNotifiedAt = sql.NullTime{Valid: true, Time: now}
	}
	return args
}

func thresholdForCond(c models.Conditions) sql.NullFloat64 {
	if c.AlertThreshold != nil {
		return sql.NullFloat64{Valid: true, Float64: *c.AlertThreshold}
	}
	if c.WarnThreshold != nil {
		return sql.NullFloat64{Valid: true, Float64: *c.WarnThreshold}
	}
	return sql.NullFloat64{}
}

func isMuted(m models.MonitorRow, now time.Time) bool {
	return m.MutedUntil.Valid && m.MutedUntil.Time.After(now)
}

func (s *Service) dispatchAll(ctx context.Context, m models.MonitorRow, cond models.Conditions, res query.ScalarResult, d expr.Decision, now time.Time) {
	var targets models.NotifyTargets
	_ = json.Unmarshal(m.NotifyJSON, &targets)
	if len(targets.ChannelIDs) == 0 {
		return
	}
	channels, err := s.repo.GetChannelsByIDs(ctx, m.TeamID, targets.ChannelIDs)
	if err != nil {
		slog.WarnContext(ctx, "alerting: load channels failed", slog.Int64("monitor_id", m.ID), slog.Any("error", err))
		return
	}
	payload := buildPayload(m, cond, res, d)
	for _, ch := range channels {
		err := s.dispatcher.Dispatch(ctx, ch, payload)
		errText := sql.NullString{}
		if err != nil {
			errText = sql.NullString{Valid: true, String: err.Error()}
			slog.WarnContext(ctx, "alerting: dispatch failed",
				slog.Int64("monitor_id", m.ID),
				slog.Int64("channel_id", ch.ID),
				slog.String("channel_type", ch.Type),
				slog.Any("error", err))
		}
		_ = s.repo.MarkChannelDelivered(ctx, ch.ID, now, errText)
	}
}

func buildPayload(m models.MonitorRow, cond models.Conditions, res query.ScalarResult, d expr.Decision) dispatch.Payload {
	threshold := 0.0
	if cond.AlertThreshold != nil {
		threshold = *cond.AlertThreshold
	} else if cond.WarnThreshold != nil {
		threshold = *cond.WarnThreshold
	}
	scopeSummary := summarizeScope(m)
	message := renderMessageBody(m, res.Value, threshold, scopeSummary, d)
	return dispatch.Payload{
		MonitorID:    m.ID,
		MonitorName:  m.Name,
		MonitorType:  m.Type,
		Priority:     m.Priority,
		Transition:   d.NewStatus,
		Status:       d.NewStatus,
		Value:        res.Value,
		Threshold:    threshold,
		ScopeSummary: scopeSummary,
		Message:      message,
		IsAlert:      d.NewStatus == "alert",
		IsWarning:    d.NewStatus == "warn",
		IsRecovery:   d.IsRecovery,
	}
}

func summarizeScope(m models.MonitorRow) string {
	var scope models.Scope
	if err := json.Unmarshal(m.ScopeJSON, &scope); err != nil || len(scope.Tags) == 0 {
		return ""
	}
	parts := make([]string, 0, len(scope.Tags))
	for _, t := range scope.Tags {
		parts = append(parts, t.Key+":"+t.Value)
	}
	return strings.Join(parts, " ")
}

func renderMessageBody(m models.MonitorRow, value, threshold float64, scopeSummary string, d expr.Decision) string {
	body := ""
	if m.MessageBody.Valid {
		body = m.MessageBody.String
	}
	if strings.TrimSpace(body) == "" {
		return defaultMessage(m, value, threshold, d)
	}
	return tmpl.Render(body, tmpl.Vars{
		Values: map[string]string{
			"value":        tmpl.FormatFloat(value),
			"threshold":    tmpl.FormatFloat(threshold),
			"service.name": serviceFromScope(m),
			"monitor.name": m.Name,
			"scope":        scopeSummary,
		},
		IsAlert:    d.NewStatus == "alert",
		IsWarning:  d.NewStatus == "warn",
		IsRecovery: d.IsRecovery,
	})
}

func defaultMessage(m models.MonitorRow, value, threshold float64, d expr.Decision) string {
	verb := "triggered"
	if d.IsRecovery {
		verb = "recovered"
	}
	return m.Name + " " + verb + " — value " + tmpl.FormatFloat(value) + " vs threshold " + tmpl.FormatFloat(threshold)
}

func serviceFromScope(m models.MonitorRow) string {
	var scope models.Scope
	if err := json.Unmarshal(m.ScopeJSON, &scope); err != nil {
		return ""
	}
	for _, t := range scope.Tags {
		if t.Key == "service" {
			return t.Value
		}
	}
	return ""
}
