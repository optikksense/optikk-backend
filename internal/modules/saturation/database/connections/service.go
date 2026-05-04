package connections

import (
	"context"
	"sort"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// GetConnectionCountSeries averages count values per (display_bucket,
// pool, state). Display-grain bucketing happens Go-side.
func (s *Service) GetConnectionCountSeries(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ConnectionCountPoint, error) {
	rows, err := s.repo.GetConnectionCountSeries(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	type key struct{ bucket, pool, state string }
	type acc struct {
		sum   float64
		count int
	}
	folded := map[key]*acc{}
	for _, r := range rows {
		k := key{bucketStr(r.Timestamp), r.PoolName, r.State}
		a, ok := folded[k]
		if !ok {
			a = &acc{}
			folded[k] = a
		}
		a.sum += r.Value
		a.count++
	}
	out := make([]ConnectionCountPoint, 0, len(folded))
	for k, a := range folded {
		v := a.sum / float64(a.count)
		out = append(out, ConnectionCountPoint{TimeBucket: k.bucket, PoolName: k.pool, State: k.state, Count: &v})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TimeBucket+"/"+out[i].PoolName+"/"+out[i].State <
			out[j].TimeBucket+"/"+out[j].PoolName+"/"+out[j].State
	})
	return out, nil
}

// GetConnectionUtilization computes used/max per (display_bucket, pool).
func (s *Service) GetConnectionUtilization(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ConnectionUtilPoint, error) {
	rows, err := s.repo.GetConnectionUtilization(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	type key struct{ bucket, pool string }
	type acc struct {
		used, max  float64
		uCnt, mCnt int
	}
	folded := map[key]*acc{}
	for _, r := range rows {
		k := key{bucketStr(r.Timestamp), r.PoolName}
		a, ok := folded[k]
		if !ok {
			a = &acc{}
			folded[k] = a
		}
		switch {
		case r.MetricName == filter.MetricDBConnectionCount && r.State == "used":
			a.used += r.Value
			a.uCnt++
		case r.MetricName == filter.MetricDBConnectionMax:
			a.max += r.Value
			a.mCnt++
		}
	}
	out := make([]ConnectionUtilPoint, 0, len(folded))
	for k, a := range folded {
		var pct *float64
		if a.mCnt > 0 && a.uCnt > 0 && a.max > 0 {
			used := a.used / float64(a.uCnt)
			max := a.max / float64(a.mCnt)
			v := used / max * 100.0
			pct = &v
		}
		out = append(out, ConnectionUtilPoint{TimeBucket: k.bucket, PoolName: k.pool, UtilPct: pct})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TimeBucket+"/"+out[i].PoolName < out[j].TimeBucket+"/"+out[j].PoolName
	})
	return out, nil
}

// GetConnectionLimits collapses the three limit gauges (max / idle.max /
// idle.min) into one row per pool, averaged over the window.
func (s *Service) GetConnectionLimits(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ConnectionLimits, error) {
	rows, err := s.repo.GetConnectionLimits(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	type acc struct {
		maxSum, idleMaxSum, idleMinSum float64
		maxCnt, idleMaxCnt, idleMinCnt int
	}
	folded := map[string]*acc{}
	for _, r := range rows {
		a, ok := folded[r.PoolName]
		if !ok {
			a = &acc{}
			folded[r.PoolName] = a
		}
		switch r.MetricName {
		case filter.MetricDBConnectionMax:
			a.maxSum += r.Value
			a.maxCnt++
		case filter.MetricDBConnectionIdleMax:
			a.idleMaxSum += r.Value
			a.idleMaxCnt++
		case filter.MetricDBConnectionIdleMin:
			a.idleMinSum += r.Value
			a.idleMinCnt++
		}
	}
	out := make([]ConnectionLimits, 0, len(folded))
	for pool, a := range folded {
		out = append(out, ConnectionLimits{
			PoolName: pool,
			Max:      avg(a.maxSum, a.maxCnt),
			IdleMax:  avg(a.idleMaxSum, a.idleMaxCnt),
			IdleMin:  avg(a.idleMinSum, a.idleMinCnt),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PoolName < out[j].PoolName })
	return out, nil
}

func (s *Service) GetPendingRequests(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]PendingRequestsPoint, error) {
	rows, err := s.repo.GetPendingRequests(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	type key struct{ bucket, pool string }
	type acc struct {
		sum   float64
		count int
	}
	folded := map[key]*acc{}
	for _, r := range rows {
		k := key{bucketStr(r.Timestamp), r.PoolName}
		a, ok := folded[k]
		if !ok {
			a = &acc{}
			folded[k] = a
		}
		a.sum += r.Value
		a.count++
	}
	out := make([]PendingRequestsPoint, 0, len(folded))
	for k, a := range folded {
		v := a.sum / float64(a.count)
		out = append(out, PendingRequestsPoint{TimeBucket: k.bucket, PoolName: k.pool, Count: &v})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TimeBucket+"/"+out[i].PoolName < out[j].TimeBucket+"/"+out[j].PoolName
	})
	return out, nil
}

// GetConnectionTimeoutRate folds the timeout counter into per-(bucket, pool)
// sums and divides by display-grain seconds for per-second rate.
func (s *Service) GetConnectionTimeoutRate(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ConnectionTimeoutPoint, error) {
	rows, err := s.repo.GetConnectionTimeoutRate(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	bucketSec := filter.BucketWidthSeconds(startMs, endMs)
	type key struct{ bucket, pool string }
	folded := map[key]float64{}
	for _, r := range rows {
		k := key{bucketStr(r.Timestamp), r.PoolName}
		folded[k] += r.Value
	}
	out := make([]ConnectionTimeoutPoint, 0, len(folded))
	for k, sum := range folded {
		rate := sum / bucketSec
		out = append(out, ConnectionTimeoutPoint{TimeBucket: k.bucket, PoolName: k.pool, TimeoutRate: &rate})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TimeBucket+"/"+out[i].PoolName < out[j].TimeBucket+"/"+out[j].PoolName
	})
	return out, nil
}

func (s *Service) GetConnectionWaitTime(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]PoolLatencyPoint, error) {
	rows, err := s.repo.GetConnectionWaitTime(ctx, teamID, startMs, endMs, f)
	return foldHist(rows), err
}

func (s *Service) GetConnectionCreateTime(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]PoolLatencyPoint, error) {
	rows, err := s.repo.GetConnectionCreateTime(ctx, teamID, startMs, endMs, f)
	return foldHist(rows), err
}

func (s *Service) GetConnectionUseTime(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]PoolLatencyPoint, error) {
	rows, err := s.repo.GetConnectionUseTime(ctx, teamID, startMs, endMs, f)
	return foldHist(rows), err
}

// foldHist shapes the per-(hour-bucket, pool) percentile rows from CH (already
// computed server-side via quantilePrometheusHistogramMerge on
// metrics_1m.latency_state) into PoolLatencyPoint records. The metrics_1m
// histogram for `db.client.connection.*time` is seconds-domain (OTel default),
// so we multiply by 1000 to convert to ms.
func foldHist(rows []histRawDTO) []PoolLatencyPoint {
	if len(rows) == 0 {
		return nil
	}
	out := make([]PoolLatencyPoint, 0, len(rows))
	for _, r := range rows {
		p50 := r.P50 * 1000.0
		p95 := r.P95 * 1000.0
		p99 := r.P99 * 1000.0
		out = append(out, PoolLatencyPoint{
			TimeBucket: bucketStr(r.Bucket),
			PoolName:   r.PoolName,
			P50Ms:      &p50,
			P95Ms:      &p95,
			P99Ms:      &p99,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TimeBucket+"/"+out[i].PoolName < out[j].TimeBucket+"/"+out[j].PoolName
	})
	return out
}

// bucketStr formats a metric timestamp into a stable per-hour label
// matching the metrics_resource ts_bucket grain (the natural display
// grain for db.client.connection.* gauges sampled at 30–60s intervals).
func bucketStr(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:00")
}

func avg(sum float64, count int) *float64 {
	if count == 0 {
		return nil
	}
	v := sum / float64(count)
	return &v
}
