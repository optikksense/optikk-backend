package apm

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/shared/displaybucket"
	"github.com/Optikk-Org/optikk-backend/internal/shared/quantile"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetRPCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.histogramSummary(ctx, teamID, startMs, endMs, MetricRPCServerDuration)
}

func (s *Service) GetMessagingPublishDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.histogramSummary(ctx, teamID, startMs, endMs, MetricMessagingPublishDuration)
}

func (s *Service) GetRPCRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	rows, err := s.repo.QueryHistogramCountSeries(ctx, teamID, startMs, endMs, MetricRPCServerDuration)
	if err != nil {
		return nil, err
	}
	return displaybucket.SumByTime(rows,
		func(r CountSeriesRow) time.Time { return r.Timestamp },
		func(r CountSeriesRow) float64 { return float64(r.Count) },
		startMs, endMs), nil
}

func (s *Service) GetProcessCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := s.repo.QueryStateSeries(ctx, teamID, startMs, endMs, MetricProcessCPUTime)
	if err != nil {
		return nil, err
	}
	return displaybucket.AvgByTimeAndKey(rows,
		func(r StateSeriesRow) time.Time { return r.Timestamp },
		func(r StateSeriesRow) string { return r.State },
		func(r StateSeriesRow) float64 { return r.Value },
		startMs, endMs), nil
}

func (s *Service) GetProcessMemory(ctx context.Context, teamID int64, startMs, endMs int64) (ProcessMemory, error) {
	rows, err := s.repo.QueryGaugeAvgByName(ctx, teamID, startMs, endMs,
		[]string{MetricProcessMemoryUsage, MetricProcessMemoryVirtual})
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
	return s.gaugeSeries(ctx, teamID, startMs, endMs, MetricProcessOpenFDs)
}

func (s *Service) GetUptime(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.gaugeSeries(ctx, teamID, startMs, endMs, MetricProcessUptime)
}

func (s *Service) histogramSummary(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (HistogramSummary, error) {
	row, err := s.repo.QueryHistogramAgg(ctx, teamID, startMs, endMs, metricName)
	if err != nil {
		return HistogramSummary{}, err
	}
	avg := 0.0
	if row.SumHistCount > 0 {
		avg = row.SumHistSum / float64(row.SumHistCount)
	}
	return HistogramSummary{
		Avg: avg,
		P50: quantile.FromHistogram(row.Buckets, row.Counts, 0.5),
		P95: quantile.FromHistogram(row.Buckets, row.Counts, 0.95),
		P99: quantile.FromHistogram(row.Buckets, row.Counts, 0.99),
	}, nil
}

func (s *Service) gaugeSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]TimeBucket, error) {
	rows, err := s.repo.QueryMetricSeries(ctx, teamID, startMs, endMs, metricName)
	if err != nil {
		return nil, err
	}
	return displaybucket.AvgByTime(rows,
		func(r MetricSeriesRow) time.Time { return r.Timestamp },
		func(r MetricSeriesRow) float64 { return r.Value },
		startMs, endMs), nil
}
