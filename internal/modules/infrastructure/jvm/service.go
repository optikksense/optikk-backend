package jvm

import (
	"cmp"
	"context"
	"math"
	"slices"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
	"github.com/Optikk-Org/optikk-backend/internal/shared/quantile"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// GetJVMMemory folds the 3-metric memory family into per-(timestamp, pool, type) rows.
func (s *Service) GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error) {
	rows, err := s.repo.QueryJVMMemoryByPool(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		ts                time.Time
		poolName, memType string
	}
	type acc struct{ used, committed, limit avgAcc }
	agg := map[key]*acc{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), poolName: r.PoolName, memType: r.MemType}
		x, ok := agg[k]
		if !ok {
			x = &acc{}
			agg[k] = x
		}
		switch r.MetricName {
		case infraconsts.MetricJVMMemoryUsed:
			x.used.add(r.Value)
		case infraconsts.MetricJVMMemoryCommitted:
			x.committed.add(r.Value)
		case infraconsts.MetricJVMMemoryLimit:
			x.limit.add(r.Value)
		}
	}
	out := make([]JVMMemoryBucket, 0, len(agg))
	for k, x := range agg {
		out = append(out, JVMMemoryBucket{
			Timestamp: formatTime(k.ts),
			PoolName:  k.poolName,
			MemType:   k.memType,
			Used:      x.used.avg(),
			Committed: x.committed.avg(),
			Limit:     x.limit.avg(),
		})
	}
	slices.SortFunc(out, func(a, b JVMMemoryBucket) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.PoolName, b.PoolName)
	})
	return out, nil
}

// GetJVMGCDuration computes P50/P95/P99 + Avg from the merged histogram via
// quantile.FromHistogram (mirror of httpmetrics' histogram-percentile path).
func (s *Service) GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := s.repo.QueryJVMGCDurationHistogram(ctx, teamID, startMs, endMs)
	if err != nil {
		return HistogramSummary{}, err
	}
	avg := 0.0
	if row.SumHistCount > 0 {
		avg = row.SumHistSum / float64(row.SumHistCount)
	}
	return HistogramSummary{
		Avg: sanitizeFloat(avg),
		P50: sanitizeFloat(quantile.FromHistogram(row.Buckets, row.Counts, 0.50)),
		P95: sanitizeFloat(quantile.FromHistogram(row.Buckets, row.Counts, 0.95)),
		P99: sanitizeFloat(quantile.FromHistogram(row.Buckets, row.Counts, 0.99)),
	}, nil
}

func (s *Service) GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMGCCollectionBucket, error) {
	rows, err := s.repo.QueryJVMGCCollectionsByName(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		ts        time.Time
		collector string
	}
	sums := map[key]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), collector: r.GCName}
		sums[k] += r.Value
	}
	out := make([]JVMGCCollectionBucket, 0, len(sums))
	for k, sum := range sums {
		v := sum
		out = append(out, JVMGCCollectionBucket{
			Timestamp: formatTime(k.ts),
			Collector: k.collector,
			Value:     sanitizeFloatPtr(&v),
		})
	}
	slices.SortFunc(out, func(a, b JVMGCCollectionBucket) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.Collector, b.Collector)
	})
	return out, nil
}

func (s *Service) GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMThreadBucket, error) {
	rows, err := s.repo.QueryJVMThreadCountByDaemon(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		ts     time.Time
		daemon string
	}
	type acc struct{ sum, count float64 }
	agg := map[key]*acc{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), daemon: r.Daemon}
		x, ok := agg[k]
		if !ok {
			x = &acc{}
			agg[k] = x
		}
		x.sum += r.Value
		x.count++
	}
	out := make([]JVMThreadBucket, 0, len(agg))
	for k, x := range agg {
		var vp *float64
		if x.count > 0 {
			v := x.sum / x.count
			vp = sanitizeFloatPtr(&v)
		}
		out = append(out, JVMThreadBucket{Timestamp: formatTime(k.ts), Daemon: k.daemon, Value: vp})
	}
	slices.SortFunc(out, func(a, b JVMThreadBucket) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.Daemon, b.Daemon)
	})
	return out, nil
}

func (s *Service) GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (JVMClassStats, error) {
	rows, err := s.repo.QueryJVMClasses(ctx, teamID, startMs, endMs)
	if err != nil {
		return JVMClassStats{}, err
	}
	var result JVMClassStats
	for _, r := range rows {
		if math.IsNaN(r.Value) || math.IsInf(r.Value, 0) {
			continue
		}
		switch r.MetricName {
		case infraconsts.MetricJVMClassLoaded:
			result.Loaded = int64(math.Round(r.Value))
		case infraconsts.MetricJVMClassCount:
			result.Count = int64(math.Round(r.Value))
		}
	}
	return result, nil
}

func (s *Service) GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (JVMCPUStats, error) {
	rows, err := s.repo.QueryJVMCPU(ctx, teamID, startMs, endMs)
	if err != nil {
		return JVMCPUStats{}, err
	}
	var result JVMCPUStats
	for _, r := range rows {
		switch r.MetricName {
		case infraconsts.MetricJVMCPUTime:
			result.CPUTimeValue = sanitizeFloat(r.Value)
		case infraconsts.MetricJVMCPUUtilization:
			result.RecentUtilization = sanitizeFloat(r.Value)
		}
	}
	return result, nil
}

func (s *Service) GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error) {
	rows, err := s.repo.QueryJVMBuffersByPool(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	type key struct {
		ts       time.Time
		poolName string
	}
	type acc struct{ memUsage, count avgAcc }
	agg := map[key]*acc{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), poolName: r.PoolName}
		x, ok := agg[k]
		if !ok {
			x = &acc{}
			agg[k] = x
		}
		switch r.MetricName {
		case infraconsts.MetricJVMBufferMemoryUsage:
			x.memUsage.add(r.Value)
		case infraconsts.MetricJVMBufferCount:
			x.count.add(r.Value)
		}
	}
	out := make([]JVMBufferBucket, 0, len(agg))
	for k, x := range agg {
		out = append(out, JVMBufferBucket{
			Timestamp:   formatTime(k.ts),
			PoolName:    k.poolName,
			MemoryUsage: x.memUsage.avg(),
			Count:       x.count.avg(),
		})
	}
	slices.SortFunc(out, func(a, b JVMBufferBucket) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.PoolName, b.PoolName)
	})
	return out, nil
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

type avgAcc struct {
	sum   float64
	count int
}

func (a *avgAcc) add(v float64) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}
	a.sum += v
	a.count++
}

func (a *avgAcc) avg() *float64 {
	if a.count == 0 {
		return nil
	}
	v := a.sum / float64(a.count)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

func sanitizeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func sanitizeFloatPtr(v *float64) *float64 {
	if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	return v
}

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
