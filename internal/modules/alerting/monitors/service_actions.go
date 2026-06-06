package monitors

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/expr"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/query"
)

// ErrNotAlerting is returned by Ack when the monitor is not currently in
// alert or warn status.
var ErrNotAlerting = errors.New("monitor is not currently alerting")

// Ack acknowledges the current alert.
func (s *Service) Ack(ctx context.Context, teamID, userID, id int64) error {
	r, ok := s.repo.(*MySQLRepository)
	if !ok {
		return errors.New("ack requires the mysql repository")
	}
	if err := r.Ack(ctx, id, teamID, userID, time.Now().UTC()); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotAlerting
		}
		return err
	}
	return nil
}

// Mute sets a mute window. duration=0 clears the mute.
func (s *Service) Mute(ctx context.Context, teamID, id int64, durationSec int) error {
	r, ok := s.repo.(*MySQLRepository)
	if !ok {
		return errors.New("mute requires the mysql repository")
	}
	var until sql.NullTime
	if durationSec > 0 {
		until = sql.NullTime{Valid: true, Time: time.Now().UTC().Add(time.Duration(durationSec) * time.Second)}
	}
	if err := r.Mute(ctx, id, teamID, until); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// Unmute clears the muted_until column. Equivalent to Mute(0).
func (s *Service) Unmute(ctx context.Context, teamID, id int64) error {
	return s.Mute(ctx, teamID, id, 0)
}

// TestResult is the dry-run payload returned by POST /monitors/:id/test.
type TestResult struct {
	Value         float64 `json:"value"`
	HasData       bool    `json:"has_data"`
	WouldDecideAs string  `json:"would_decide_as"`
	Threshold     float64 `json:"threshold"`
}

// Test evaluates the monitor once without changing state.
func (s *Service) Test(ctx context.Context, teamID, id int64, queries query.Registry) (TestResult, error) {
	row, state, err := s.repo.GetByID(ctx, id, teamID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TestResult{}, ErrNotFound
		}
		return TestResult{}, err
	}
	cond := query.DecodeConditions(row)
	q := query.DecodeQuery(row)
	scope := query.DecodeScope(row)
	backend, err := queries.For(row.Type)
	if err != nil {
		return TestResult{}, err
	}
	res, err := backend.Scalar(ctx, row, q, scope, cond, time.Now().UTC())
	if err != nil {
		return TestResult{}, err
	}
	renotify := int64(0)
	if row.RenotifyEverySec.Valid {
		renotify = row.RenotifyEverySec.Int64
	}
	d := expr.Decide(state, row, cond, res.Value, res.HasData, renotify, time.Now().UTC())
	threshold := 0.0
	if cond.AlertThreshold != nil {
		threshold = *cond.AlertThreshold
	} else if cond.WarnThreshold != nil {
		threshold = *cond.WarnThreshold
	}
	return TestResult{
		Value:         res.Value,
		HasData:       res.HasData,
		WouldDecideAs: d.NewStatus,
		Threshold:     threshold,
	}, nil
}

// Series returns the eval-chart timeseries for the detail page.
func (s *Service) Series(ctx context.Context, teamID, id int64, queries query.Registry, windowMs int64) (SeriesResponse, error) {
	row, _, err := s.repo.GetByID(ctx, id, teamID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SeriesResponse{}, ErrNotFound
		}
		return SeriesResponse{}, err
	}
	cond := query.DecodeConditions(row)
	q := query.DecodeQuery(row)
	scope := query.DecodeScope(row)
	backend, err := queries.For(row.Type)
	if err != nil {
		return SeriesResponse{}, err
	}
	points, err := backend.Series(ctx, row, q, scope, cond, windowMs, time.Now().UTC())
	if err != nil {
		return SeriesResponse{}, err
	}
	return SeriesResponse{
		Points:            points,
		AlertThreshold:    cond.AlertThreshold,
		WarnThreshold:     cond.WarnThreshold,
		RecoveryThreshold: cond.RecoveryThreshold,
	}, nil
}

// SeriesResponse pairs bucketed values with the monitor's thresholds.
type SeriesResponse struct {
	Points            []query.Point `json:"points"`
	AlertThreshold    *float64      `json:"alert_threshold,omitempty"`
	WarnThreshold     *float64      `json:"warn_threshold,omitempty"`
	RecoveryThreshold *float64      `json:"recovery_threshold,omitempty"`
}

// Events returns recent events for a single monitor.
func (s *Service) Events(ctx context.Context, teamID, id int64, limit int) ([]MonitorEventResponse, error) {
	r, ok := s.repo.(*MySQLRepository)
	if !ok {
		return nil, errors.New("events requires the mysql repository")
	}
	rows, err := r.Events(ctx, id, teamID, limit)
	if err != nil {
		return nil, err
	}
	return toEventResponses(rows), nil
}

// Activity returns recent events across all team monitors.
func (s *Service) Activity(ctx context.Context, teamID int64, sinceMs int64, limit int) ([]MonitorEventResponse, error) {
	r, ok := s.repo.(*MySQLRepository)
	if !ok {
		return nil, errors.New("activity requires the mysql repository")
	}
	since := time.Now().UTC().Add(-1 * time.Hour)
	if sinceMs > 0 {
		since = time.UnixMilli(sinceMs).UTC()
	}
	rows, err := r.Activity(ctx, teamID, since, limit)
	if err != nil {
		return nil, err
	}
	return toEventResponses(rows), nil
}

// StatusTimeline returns 24h status bands derived from monitor_events.
func (s *Service) StatusTimeline(ctx context.Context, teamID, id int64, windowMs int64) (StatusTimelineResponse, error) {
	r, ok := s.repo.(*MySQLRepository)
	if !ok {
		return StatusTimelineResponse{}, errors.New("status-timeline requires the mysql repository")
	}
	if windowMs <= 0 {
		windowMs = 24 * 60 * 60 * 1000
	}
	since := time.Now().UTC().Add(-time.Duration(windowMs) * time.Millisecond)
	rows, err := r.StatusTimelineRows(ctx, id, teamID, since)
	if err != nil {
		return StatusTimelineResponse{}, err
	}
	return StatusTimelineResponse{
		Bands:     buildBands(rows, since, time.Now().UTC()),
		StartedAt: since,
		EndedAt:   time.Now().UTC(),
	}, nil
}

// StatusTimelineResponse is the 24h bar of status bands.
type StatusTimelineResponse struct {
	Bands     []StatusBand `json:"bands"`
	StartedAt time.Time    `json:"started_at"`
	EndedAt   time.Time    `json:"ended_at"`
}

// StatusBand is one contiguous span of one status.
type StatusBand struct {
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
}

// buildBands materializes the start-to-end window as a sequence of bands
// by walking events in chronological order.
func buildBands(events []models.MonitorEventRow, start, end time.Time) []StatusBand {
	if len(events) == 0 {
		return []StatusBand{{Status: "ok", StartedAt: start, EndedAt: end}}
	}
	bands := make([]StatusBand, 0, len(events)*2+1)
	cursor := start
	current := "ok"
	for _, e := range events {
		if e.StartedAt.Before(cursor) {
			continue
		}
		if e.StartedAt.After(cursor) {
			bands = append(bands, StatusBand{Status: current, StartedAt: cursor, EndedAt: e.StartedAt})
		}
		switch e.Kind {
		case "triggered":
			current = "alert"
		case "recovered":
			current = "ok"
		case "muted":
			current = "no_data"
		}
		cursor = e.StartedAt
	}
	if cursor.Before(end) {
		bands = append(bands, StatusBand{Status: current, StartedAt: cursor, EndedAt: end})
	}
	return bands
}
