package apm

import (
	"context"
	"time"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetRPCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.QueryRPCDurationHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return histogramSummary(row), nil
}

func (s *Service) GetMessagingPublishDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.QueryMessagingPublishDurationHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	return histogramSummary(row), nil
}

func (s *Service) GetRPCRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	rows, err := s.repo.QueryRPCRequestCountSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]TimeBucket, len(rows))
	for i, r := range rows {
		v := float64(r.Count)
		out[i] = TimeBucket{Timestamp: formatBucket(r.Timestamp), Value: &v}
	}
	return out, nil
}

func (s *Service) GetProcessCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := s.repo.QueryProcessCPUStateSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]StateBucket, len(rows))
	for i, r := range rows {
		v := r.Value
		out[i] = StateBucket{Timestamp: formatBucket(r.Timestamp), State: r.State, Value: &v}
	}
	return out, nil
}

func (s *Service) GetProcessMemory(ctx context.Context, teamID int64, startMs, endMs int64) (ProcessMemory, error) {
	rows, err := s.repo.QueryProcessMemoryAvg(ctx, teamID, startMs, endMs)
	if err != nil {
		return ProcessMemory{}, err
	}
	var out ProcessMemory
	for _, r := range rows {
		switch r.MetricName {
		case MetricProcessMemoryUsage:
			out.RSS = r.Avg
		case MetricProcessMemoryVirtual:
			out.VMS = r.Avg
		}
	}
	return out, nil
}

func (s *Service) GetOpenFDs(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	rows, err := s.repo.QueryProcessOpenFDsSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldGaugeSeries(rows, startMs, endMs), nil
}

func (s *Service) GetUptime(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	rows, err := s.repo.QueryProcessUptimeSeries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldGaugeSeries(rows, startMs, endMs), nil
}

func histogramSummary(row HistogramAggRow) HistogramSummary {
	avg := 0.0
	if row.SumHistCount > 0 {
		avg = row.SumHistSum / float64(row.SumHistCount)
	}
	return HistogramSummary{
		Avg: avg,
		P50: row.P50,
		P95: row.P95,
		P99: row.P99,
	}
}

func foldGaugeSeries(rows []MetricSeriesRow, _, _ int64) []TimeBucket {
	out := make([]TimeBucket, len(rows))
	for i, r := range rows {
		v := r.Value
		out[i] = TimeBucket{Timestamp: formatBucket(r.Timestamp), Value: &v}
	}
	return out
}

// formatBucket renders a CH-emitted display-bucket DateTime as a stable
// UTC label. Repos already round to the display grain via
// timebucket.DisplayGrainSQL, so this is pure formatting.
func formatBucket(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
