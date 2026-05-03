package disk

import (
	"cmp"
	"context"
	"math"
	"slices"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetDiskIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	rows, err := s.repo.QueryDiskIOByDirection(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldDirectionBuckets(rows, startMs, endMs), nil
}

func (s *Service) GetDiskOperations(ctx context.Context, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	rows, err := s.repo.QueryDiskOperationsByDirection(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldDirectionBuckets(rows, startMs, endMs), nil
}

func (s *Service) GetDiskIOTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	rows, err := s.repo.QueryDiskIOTimeTotal(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldResourceBucketsCounter(rows, startMs, endMs), nil
}

func (s *Service) GetFilesystemUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]MountpointBucket, error) {
	rows, err := s.repo.QueryFilesystemUsageByMountpoint(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldMountpointBuckets(rows, startMs, endMs), nil
}

func (s *Service) GetFilesystemUtilization(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	rows, err := s.repo.QueryFilesystemUtilizationTotal(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return foldResourceBucketsGauge(rows, startMs, endMs), nil
}

func (s *Service) GetAvgDisk(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	rows, err := s.repo.QueryDiskUtilizationAgg(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{}, err
	}
	avg := foldDiskMetricRows(rows)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (s *Service) GetDiskByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	rows, err := s.repo.QueryDiskUtilizationForService(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return foldDiskMetricRows(rows), nil
}

func (s *Service) GetDiskByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	_ = container // not currently used as a CH filter; preserved in signature for caller symmetry.
	rows, err := s.repo.QueryDiskUtilizationForInstance(ctx, teamID, startMs, endMs, host, pod, serviceName)
	if err != nil {
		return nil, err
	}
	return foldDiskMetricRows(rows), nil
}

// ---------------------------------------------------------------------------
// Folds + normalization.
// ---------------------------------------------------------------------------

func foldDirectionBuckets(rows []DiskDirectionRow, startMs, endMs int64) []DirectionBucket {
	type key struct {
		ts        time.Time
		direction string
	}
	sums := map[key]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), direction: r.Direction}
		sums[k] += r.Value
	}
	out := make([]DirectionBucket, 0, len(sums))
	for k, sum := range sums {
		v := sum
		out = append(out, DirectionBucket{Timestamp: formatTime(k.ts), Direction: k.direction, Value: &v})
	}
	slices.SortFunc(out, func(a, b DirectionBucket) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.Direction, b.Direction)
	})
	return out
}

func foldMountpointBuckets(rows []DiskMountpointRow, startMs, endMs int64) []MountpointBucket {
	type key struct {
		ts         time.Time
		mountpoint string
	}
	type acc struct{ sum, count float64 }
	agg := map[key]*acc{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := key{ts: timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs), mountpoint: r.Mountpoint}
		x, ok := agg[k]
		if !ok {
			x = &acc{}
			agg[k] = x
		}
		x.sum += r.Value
		x.count++
	}
	out := make([]MountpointBucket, 0, len(agg))
	for k, x := range agg {
		var vp *float64
		if x.count > 0 {
			v := x.sum / x.count
			vp = &v
		}
		out = append(out, MountpointBucket{Timestamp: formatTime(k.ts), Mountpoint: k.mountpoint, Value: vp})
	}
	slices.SortFunc(out, func(a, b MountpointBucket) int {
		if c := cmp.Compare(a.Timestamp, b.Timestamp); c != 0 {
			return c
		}
		return cmp.Compare(a.Mountpoint, b.Mountpoint)
	})
	return out
}

func foldResourceBucketsCounter(rows []DiskValueRow, startMs, endMs int64) []ResourceBucket {
	sums := map[time.Time]float64{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs)
		sums[k] += r.Value
	}
	out := make([]ResourceBucket, 0, len(sums))
	for ts, sum := range sums {
		v := sum
		out = append(out, ResourceBucket{Timestamp: formatTime(ts), Pod: "", Value: &v})
	}
	slices.SortFunc(out, func(a, b ResourceBucket) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	return out
}

func foldResourceBucketsGauge(rows []DiskValueRow, startMs, endMs int64) []ResourceBucket {
	type acc struct{ sum, count float64 }
	agg := map[time.Time]*acc{}
	windowMs := endMs - startMs
	for _, r := range rows {
		k := timebucket.DisplayBucket(r.Timestamp.Unix(), windowMs)
		x, ok := agg[k]
		if !ok {
			x = &acc{}
			agg[k] = x
		}
		x.sum += r.Value
		x.count++
	}
	out := make([]ResourceBucket, 0, len(agg))
	for ts, x := range agg {
		var vp *float64
		if x.count > 0 {
			v := x.sum / x.count
			vp = &v
		}
		out = append(out, ResourceBucket{Timestamp: formatTime(ts), Pod: "", Value: vp})
	}
	slices.SortFunc(out, func(a, b ResourceBucket) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	return out
}

// foldDiskMetricRows blends the disk utilization family. Only metrics whose
// values lie in the percentage range [0, 100] are kept; raw byte counts from
// disk.free / disk.total are filtered out by the > PercentageThreshold*100
// rejection (preserves the original behaviour exactly).
func foldDiskMetricRows(rows []DiskMetricNameRow) *float64 {
	byMetric := map[string]float64{}
	for _, r := range rows {
		byMetric[r.MetricName] = r.Value
	}
	var values []float64
	add := func(v float64) {
		if nv := normalizeUtilization(v); nv != nil {
			values = append(values, *nv)
		}
	}
	if v, ok := byMetric[infraconsts.MetricSystemDiskUtilization]; ok {
		add(v)
	}
	if v, ok := byMetric[infraconsts.MetricDiskFree]; ok {
		add(v)
	}
	if v, ok := byMetric[infraconsts.MetricDiskTotal]; ok {
		add(v)
	}
	return averageFloats(values)
}

func normalizeUtilization(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > infraconsts.PercentageThreshold*100 {
		return nil
	}
	if v <= infraconsts.PercentageThreshold {
		v = v * infraconsts.PercentageMultiplier
	}
	return &v
}

func averageFloats(values []float64) *float64 {
	var sum float64
	count := 0
	for _, v := range values {
		if !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 {
			sum += v
			count++
		}
	}
	if count == 0 {
		return nil
	}
	avg := sum / float64(count)
	return &avg
}

func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}
