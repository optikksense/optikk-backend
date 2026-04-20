package apm

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
)

type Service struct {
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) *Service {
	return &Service{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

func (s *Service) GetRPCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.GetRPCDuration(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	// No SpanLatency / HttpLatency sketch is populated for rpc.server.duration;
	// percentiles stay 0 until a dedicated rpc sketch kind is wired on ingest.
	return histogramSummaryFrom(row), nil
}

func (s *Service) GetRPCRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetRPCRequestRate(ctx, teamID, startMs, endMs)
}

func (s *Service) GetMessagingPublishDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.GetMessagingPublishDuration(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	// Generic messaging.client.operation.duration has no matching sketch kind
	// (Kafka-specific latency lives in sketch.KafkaTopicLatency). Percentiles
	// remain 0 until a generic messaging sketch exists.
	return histogramSummaryFrom(row), nil
}

func (s *Service) GetProcessCPU(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := s.repo.GetProcessCPU(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]StateBucket, len(rows))
	for i, row := range rows {
		avg := safeAvg(row.ValSum, row.ValCount)
		out[i] = StateBucket{
			Timestamp: row.Timestamp,
			State:     row.State,
			Value:     &avg,
		}
	}
	return out, nil
}

func (s *Service) GetProcessMemory(ctx context.Context, teamID int64, startMs, endMs int64) (ProcessMemory, error) {
	row, err := s.repo.GetProcessMemory(ctx, teamID, startMs, endMs)
	if err != nil {
		return ProcessMemory{}, err
	}
	return ProcessMemory{
		RSS: safeAvg(row.RSSSum, row.RSSCount),
		VMS: safeAvg(row.VMSSum, row.VMSCount),
	}, nil
}

func (s *Service) GetOpenFDs(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	rows, err := s.repo.GetOpenFDs(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return timeBucketsFrom(rows), nil
}

func (s *Service) GetUptime(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	rows, err := s.repo.GetUptime(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return timeBucketsFrom(rows), nil
}

func histogramSummaryFrom(row histogramSummaryRow) HistogramSummary {
	avg := 0.0
	if row.HistCount > 0 {
		avg = row.HistSum / float64(row.HistCount)
	}
	return HistogramSummary{
		P50: row.P50,
		P95: row.P95,
		P99: row.P99,
		Avg: avg,
	}
}

func timeBucketsFrom(rows []timeBucketRow) []TimeBucket {
	out := make([]TimeBucket, len(rows))
	for i, row := range rows {
		avg := safeAvg(row.ValSum, row.ValCount)
		out[i] = TimeBucket{
			Timestamp: row.Timestamp,
			Value:     &avg,
		}
	}
	return out
}

func safeAvg(sum float64, count int64) float64 {
	if count <= 0 {
		return 0
	}
	return sum / float64(count)
}
