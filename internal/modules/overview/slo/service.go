package slo

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
)

const (
	availabilityTarget = 99.9
	p95LatencyTargetMs = 300.0
)

type Service interface {
	GetSloSli(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*Response, error)
	GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error)
	GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error)
}

type SLOService struct {
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) Service {
	return &SLOService{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

func (s *SLOService) GetSloSli(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*Response, error) {
	row, err := s.repo.GetSummary(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		slog.Error("slo: GetSloSli summary failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("service", serviceName))
		return nil, err
	}

	summary := summaryFromRow(row)
	s.fillSummaryP95(ctx, &summary, teamID, startMs, endMs, serviceName)

	timeseriesRows, err := s.repo.GetTimeSeries(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		slog.Error("slo: GetSloSli timeseries failed", slog.Any("error", err), slog.Int64("team_id", teamID), slog.String("service", serviceName))
		return nil, err
	}
	timeseries := timeseriesFromRows(timeseriesRows)

	errorBudgetRemaining := remainingErrorBudgetPercent(summary.AvailabilityPercent)

	return &Response{
		Objectives: Objectives{
			AvailabilityTarget: availabilityTarget,
			P95LatencyTargetMs: p95LatencyTargetMs,
		},
		Status: Status{
			AvailabilityPercent:         summary.AvailabilityPercent,
			P95LatencyMs:                summary.P95LatencyMs,
			ErrorBudgetRemainingPercent: errorBudgetRemaining,
			Compliant:                   summary.AvailabilityPercent >= availabilityTarget && summary.P95LatencyMs <= p95LatencyTargetMs,
		},
		Summary:    summary,
		Timeseries: timeseries,
	}, nil
}

func (s *SLOService) GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error) {
	return s.repo.GetBurnDown(ctx, teamID, startMs, endMs, serviceName)
}

func (s *SLOService) GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error) {
	// Fast burn: last 5 minutes error rate
	fastRate, err := s.repo.ErrorRateForWindow(ctx, teamID, 5, serviceName)
	if err != nil {
		return nil, err
	}
	slowRate, err := s.repo.ErrorRateForWindow(ctx, teamID, 60, serviceName)
	if err != nil {
		return nil, err
	}

	row, err := s.repo.GetSummary(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	summary := summaryFromRow(row)
	s.fillSummaryP95(ctx, &summary, teamID, startMs, endMs, serviceName)

	return &BurnRate{
		FastBurnRate:    fastRate,
		SlowBurnRate:    slowRate,
		FastWindow:      "5m",
		SlowWindow:      "1h",
		BudgetRemaining: remainingErrorBudgetPercent(summary.AvailabilityPercent),
	}, nil
}

// fillSummaryP95 merges SpanLatencyService sketches (either for a single service
// or across every service in the tenant) and writes the resulting p95 onto the
// summary. Zero-div is guarded; sketch misses leave the zero placeholder in place.
func (s *SLOService) fillSummaryP95(ctx context.Context, summary *Summary, teamID int64, startMs, endMs int64, serviceName string) {
	if s.sketchQ == nil {
		return
	}
	if serviceName != "" {
		pcts, _ := s.sketchQ.Percentiles(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, 0.95)
		if v, ok := pcts[sketch.DimSpanService(serviceName)]; ok && len(v) == 1 {
			summary.P95LatencyMs = v[0]
		}
		return
	}
	pcts, _ := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, []string{""}, 0.95)
	if v, ok := pcts[""]; ok && len(v) == 1 {
		summary.P95LatencyMs = v[0]
	}
}

func summaryFromRow(row summaryRow) Summary {
	availability := 100.0
	if row.TotalRequests > 0 {
		availability = float64(row.TotalRequests-row.ErrorCount) * 100.0 / float64(row.TotalRequests)
	}
	avg := 0.0
	if row.LatencyCount > 0 {
		avg = row.LatencySumMs / float64(row.LatencyCount)
	}
	return Summary{
		TotalRequests:       row.TotalRequests,
		ErrorCount:          row.ErrorCount,
		AvailabilityPercent: availability,
		AvgLatencyMs:        avg,
		P95LatencyMs:        row.P95LatencyMs,
	}
}

func timeseriesFromRows(rows []timeSliceRow) []TimeSlice {
	slices := make([]TimeSlice, len(rows))
	for i, row := range rows {
		avg := 0.0
		if row.LatencyCount > 0 {
			avg = row.LatencySumMs / float64(row.LatencyCount)
		}
		slices[i] = TimeSlice{
			Timestamp:           row.TimeBucket,
			RequestCount:        row.RequestCount,
			ErrorCount:          row.ErrorCount,
			AvailabilityPercent: row.AvailabilityPercent,
			AvgLatencyMs:        &avg,
		}
	}
	return slices
}

func remainingErrorBudgetPercent(availabilityPercent float64) float64 {
	totalBudget := 100.0 - availabilityTarget
	if totalBudget <= 0 {
		return 100.0
	}

	burned := 100.0 - availabilityPercent
	remaining := (totalBudget - burned) * 100.0 / totalBudget
	switch {
	case remaining < 0:
		return 0
	case remaining > 100:
		return 100
	default:
		return remaining
	}
}
