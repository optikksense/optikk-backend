package jvm

import (
	"context"
	"fmt"
	"math"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
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

func (s *Service) GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error) {
	rows, err := s.repo.GetJVMMemory(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]JVMMemoryBucket, len(rows))
	for i, row := range rows {
		used := avgPtr(row.UsedSum, row.UsedCount)
		committed := avgPtr(row.CommittedSum, row.CommittedCount)
		limit := avgPtr(row.LimitSum, row.LimitCount)
		out[i] = JVMMemoryBucket{
			Timestamp: row.Timestamp,
			PoolName:  row.PoolName,
			MemType:   row.MemType,
			Used:      sanitizeFloatPtr(used),
			Committed: sanitizeFloatPtr(committed),
			Limit:     sanitizeFloatPtr(limit),
		}
	}
	return out, nil
}

func (s *Service) GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.GetJVMGCDuration(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	summary := HistogramSummary{
		P50: sanitizeFloat(row.P50),
		P95: sanitizeFloat(row.P95),
		P99: sanitizeFloat(row.P99),
		Avg: sanitizeFloat(safeAvgFloat(row.HistSum, int64(row.HistCount))), //nolint:gosec // count fits int64
	}
	if s.sketchQ != nil {
		// JvmMetricLatency dim = <metric_name>|<service_name>|<pod>; merging
		// across every service+pod for the tenant gives the tenant-wide p50/95/99
		// for jvm.gc.duration.
		prefix := sketch.DimJvmMetric(infraconsts.MetricJVMGCDuration, "", "")
		// Strip the trailing empty segments so we match every service|pod tuple.
		// DimJvmMetric("jvm.gc.duration","","") -> "jvm.gc.duration||" — prefix
		// "jvm.gc.duration|" captures all variants (including the empty one).
		prefix = prefix[:len(infraconsts.MetricJVMGCDuration)+1]
		pcts, _ := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.JvmMetricLatency, teamIDString(teamID), startMs, endMs, []string{prefix}, 0.5, 0.95, 0.99)
		if v, ok := pcts[prefix]; ok && len(v) == 3 {
			summary.P50 = sanitizeFloat(v[0])
			summary.P95 = sanitizeFloat(v[1])
			summary.P99 = sanitizeFloat(v[2])
		}
	}
	return summary, nil
}

func (s *Service) GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMGCCollectionBucket, error) {
	return s.repo.GetJVMGCCollections(ctx, teamID, startMs, endMs)
}

func (s *Service) GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMThreadBucket, error) {
	rows, err := s.repo.GetJVMThreadCount(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]JVMThreadBucket, len(rows))
	for i, row := range rows {
		v := avgPtr(row.ValSum, int64(row.ValCount)) //nolint:gosec // count fits int64
		out[i] = JVMThreadBucket{
			Timestamp: row.Timestamp,
			Daemon:    row.Daemon,
			Value:     sanitizeFloatPtr(v),
		}
	}
	return out, nil
}

func (s *Service) GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (JVMClassStats, error) {
	row, err := s.repo.GetJVMClasses(ctx, teamID, startMs, endMs)
	if err != nil {
		return JVMClassStats{}, err
	}
	countAvg := safeAvgFloat(row.CountSum, row.CountN)
	return JVMClassStats{
		Loaded: roundFinite(row.LoadedSum),
		Count:  roundFinite(countAvg),
	}, nil
}

func (s *Service) GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (JVMCPUStats, error) {
	row, err := s.repo.GetJVMCPU(ctx, teamID, startMs, endMs)
	if err != nil {
		return JVMCPUStats{}, err
	}
	return JVMCPUStats{
		CPUTimeValue:      sanitizeFloat(row.CPUTimeSum),
		RecentUtilization: sanitizeFloat(safeAvgFloat(row.CPUUtilSum, row.CPUUtilN)),
	}, nil
}

func (s *Service) GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error) {
	rows, err := s.repo.GetJVMBuffers(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]JVMBufferBucket, len(rows))
	for i, row := range rows {
		mem := avgPtr(row.MemoryUsageSum, row.MemoryUsageCount)
		cnt := avgPtr(row.BufCountSum, row.BufCountN)
		out[i] = JVMBufferBucket{
			Timestamp:   row.Timestamp,
			PoolName:    row.PoolName,
			MemoryUsage: sanitizeFloatPtr(mem),
			Count:       sanitizeFloatPtr(cnt),
		}
	}
	return out, nil
}

// avgPtr returns a *float64 pointing at sum/count, or nil when count is zero.
// Null pointer preserves the existing wire contract (pool with no samples ->
// null in JSON), matching the old avgIf() behavior that also returned NaN.
func avgPtr(sum float64, count int64) *float64 {
	if count <= 0 {
		return nil
	}
	v := sum / float64(count)
	return &v
}

func safeAvgFloat(sum float64, count int64) float64 {
	if count <= 0 {
		return 0
	}
	return sum / float64(count)
}

func roundFinite(v float64) int64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return int64(math.Round(v))
}
