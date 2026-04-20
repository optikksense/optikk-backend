package kubernetes

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"golang.org/x/sync/errgroup"
)

type Repository interface {
	GetContainerCPU(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]containerBucketDTO, error)
	GetCPUThrottling(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]containerBucketDTO, error)
	GetContainerMemory(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]containerBucketDTO, error)
	GetOOMKills(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]containerBucketDTO, error)
	GetPodRestarts(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]podStatDTO, error)
	GetNodeAllocatable(ctx context.Context, teamID int64, startMs, endMs int64, node string) (nodeAllocatableDTO, error)
	GetPodPhases(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]phaseStatDTO, error)
	GetReplicaStatus(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]replicaStatDTO, error)
	GetVolumeUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]volumeStatDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

// containerBucketSumCountRow is the CH scan target for the container-bucket
// endpoints. Go divides Sum/Count → emits a *float64.
type containerBucketSumCountRow struct {
	Timestamp string  `ch:"time_bucket"`
	Container string  `ch:"container"`
	Sum       float64 `ch:"s"`
	Count     uint64  `ch:"c"`
}

// podRestartRow returns the raw max-value as float64; Go rounds to int64.
type podRestartRow struct {
	PodName   string  `ch:"pod_name"`
	Namespace string  `ch:"namespace"`
	Restarts  float64 `ch:"restarts"`
}

// nodeAllocatableLegRow is one leg of the split allocatable scan.
type nodeAllocatableLegRow struct {
	Sum   float64 `ch:"s"`
	Count uint64  `ch:"c"`
}

// phaseCountRow is the CH scan target for the pod-phases endpoint; the raw
// count is a uint64 and the service-facing DTO gets it cast Go-side.
type phaseCountRow struct {
	Phase string `ch:"phase"`
	Count uint64 `ch:"c"`
}

// replicaLegRow holds (replica_set, sum, count) for a single metric leg of
// the replica-status split scan.
type replicaLegRow struct {
	ReplicaSet string  `ch:"replica_set"`
	Sum        float64 `ch:"s"`
	Count      uint64  `ch:"c"`
}

// volumeLegRow is the per-volume leg row for the volume-usage split scan.
type volumeLegRow struct {
	VolumeName string  `ch:"volume_name"`
	Sum        float64 `ch:"s"`
	Count      uint64  `ch:"c"`
}

func nodeFilter(node string) (clause string, args []any) {
	if node == "" {
		return "", nil
	}
	return " AND attributes.'k8s.node.name'::String = @node", []any{clickhouse.Named("node", node)}
}

func divideOrNilCount(sum float64, count uint64) *float64 {
	if count == 0 {
		return nil
	}
	v := sum / float64(count)
	return &v
}

func (r *ClickHouseRepository) queryContainerBuckets(ctx context.Context, teamID int64, startMs, endMs int64, metricName string, node string) ([]containerBucketDTO, error) {
	b := timebucket.Expression(startMs, endMs)
	containerAttr := attrString(AttrContainerName)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s                 AS time_bucket,
		    %s                 AS container,
		    sum(value)         AS s,
		    count()            AS c
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'%s
		GROUP BY time_bucket, container
		ORDER BY time_bucket, container
	`,
		b, containerAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, metricName, nf,
	)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)
	var rows []containerBucketSumCountRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]containerBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = ContainerBucket{
			Timestamp: row.Timestamp,
			Container: row.Container,
			Value:     divideOrNilCount(row.Sum, row.Count),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetContainerCPU(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]containerBucketDTO, error) {
	return r.queryContainerBuckets(ctx, teamID, startMs, endMs, MetricContainerCPUTime, node)
}

func (r *ClickHouseRepository) GetCPUThrottling(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]containerBucketDTO, error) {
	return r.queryContainerBuckets(ctx, teamID, startMs, endMs, MetricContainerCPUThrottledTime, node)
}

func (r *ClickHouseRepository) GetContainerMemory(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]containerBucketDTO, error) {
	return r.queryContainerBuckets(ctx, teamID, startMs, endMs, MetricContainerMemoryUsage, node)
}

func (r *ClickHouseRepository) GetOOMKills(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]containerBucketDTO, error) {
	return r.queryContainerBuckets(ctx, teamID, startMs, endMs, MetricContainerOOMKillCount, node)
}

func (r *ClickHouseRepository) GetPodRestarts(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]podStatDTO, error) {
	podAttr := attrString(AttrK8sPodName)
	nsAttr := attrString(AttrK8sNamespace)
	nf, nfArgs := nodeFilter(node)

	// `max()` already returns a Float64 (the native column type); the Go side
	// casts to int64 so the DTO stays typed.
	query := fmt.Sprintf(`
		SELECT
		    %s           AS pod_name,
		    %s           AS namespace,
		    max(value)   AS restarts
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'%s
		GROUP BY pod_name, namespace
		ORDER BY restarts DESC
		LIMIT 100
	`,
		podAttr, nsAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sContainerRestarts, nf,
	)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)
	var raw []podRestartRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	out := make([]podStatDTO, len(raw))
	for i, row := range raw {
		out[i] = PodStat{
			PodName:   row.PodName,
			Namespace: row.Namespace,
			Restarts:  int64(row.Restarts),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetNodeAllocatable(ctx context.Context, teamID int64, startMs, endMs int64, node string) (nodeAllocatableDTO, error) {
	nf, nfArgs := nodeFilter(node)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)

	var (
		cpu nodeAllocatableLegRow
		mem nodeAllocatableLegRow
	)
	g, gctx := errgroup.WithContext(ctx)

	runLeg := func(metric string, dest *nodeAllocatableLegRow) func() error {
		return func() error {
			query := fmt.Sprintf(`
				SELECT sum(value) AS s, count() AS c
				FROM %s
				WHERE %s = @teamID
				  AND %s BETWEEN @start AND @end
				  AND %s = '%s'%s`,
				TableMetrics,
				ColTeamID, ColTimestamp,
				ColMetricName, metric, nf)
			return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(dest)
		}
	}
	g.Go(runLeg(MetricK8sNodeAllocatableCPU, &cpu))
	g.Go(runLeg(MetricK8sNodeAllocatableMemory, &mem))

	if err := g.Wait(); err != nil {
		return nodeAllocatableDTO{}, err
	}

	out := nodeAllocatableDTO{}
	if cpu.Count > 0 {
		out.CPUCores = cpu.Sum / float64(cpu.Count)
	}
	if mem.Count > 0 {
		out.MemoryBytes = mem.Sum / float64(mem.Count)
	}
	return out, nil
}

func (r *ClickHouseRepository) GetPodPhases(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]phaseStatDTO, error) {
	phaseAttr := attrString(AttrK8sPodPhase)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s           AS phase,
		    count()      AS c
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'%s
		GROUP BY phase
		ORDER BY c DESC
	`,
		phaseAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sPodPhase, nf,
	)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)
	var raw []phaseCountRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	out := make([]phaseStatDTO, len(raw))
	for i, row := range raw {
		out[i] = PhaseStat{
			Phase: row.Phase,
			Count: int64(row.Count), //nolint:gosec // G115
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetReplicaStatus(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]replicaStatDTO, error) {
	rsAttr := attrString(AttrReplicaSetName)
	nf, nfArgs := nodeFilter(node)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)

	var (
		desired   []replicaLegRow
		available []replicaLegRow
	)
	g, gctx := errgroup.WithContext(ctx)

	runLeg := func(metric string, dest *[]replicaLegRow) func() error {
		return func() error {
			query := fmt.Sprintf(`
				SELECT
				    %s            AS replica_set,
				    sum(value)    AS s,
				    count()       AS c
				FROM %s
				WHERE %s = @teamID
				  AND %s BETWEEN @start AND @end
				  AND %s = '%s'%s
				GROUP BY replica_set`,
				rsAttr,
				TableMetrics,
				ColTeamID, ColTimestamp,
				ColMetricName, metric, nf)
			return r.db.Select(dbutil.OverviewCtx(gctx), dest, query, args...)
		}
	}
	g.Go(runLeg(MetricK8sReplicaSetDesired, &desired))
	g.Go(runLeg(MetricK8sReplicaSetAvailable, &available))

	if err := g.Wait(); err != nil {
		return nil, err
	}

	type cell struct {
		desired, available *float64
	}
	cells := make(map[string]*cell)
	keys := make([]string, 0)
	ensure := func(rs string) *cell {
		if c, ok := cells[rs]; ok {
			return c
		}
		c := &cell{}
		cells[rs] = c
		keys = append(keys, rs)
		return c
	}
	for _, row := range desired {
		c := ensure(row.ReplicaSet)
		if row.Count > 0 {
			v := row.Sum / float64(row.Count)
			c.desired = &v
		}
	}
	for _, row := range available {
		c := ensure(row.ReplicaSet)
		if row.Count > 0 {
			v := row.Sum / float64(row.Count)
			c.available = &v
		}
	}

	// Stable output ordered by replica_set name (mirrors the previous
	// ORDER BY replica_set).
	sortedKeys := sortStrings(keys)
	out := make([]replicaStatDTO, 0, len(sortedKeys))
	for _, rs := range sortedKeys {
		c := cells[rs]
		stat := ReplicaStat{ReplicaSet: rs}
		if c.desired != nil {
			stat.Desired = int64(*c.desired)
		}
		if c.available != nil {
			stat.Available = int64(*c.available)
		}
		out = append(out, stat)
	}
	return out, nil
}

func (r *ClickHouseRepository) GetVolumeUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]volumeStatDTO, error) {
	volAttr := attrString(AttrK8sVolumeName)
	nf, nfArgs := nodeFilter(node)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)

	var (
		capacity []volumeLegRow
		inodes   []volumeLegRow
	)
	g, gctx := errgroup.WithContext(ctx)

	runLeg := func(metric string, dest *[]volumeLegRow) func() error {
		return func() error {
			query := fmt.Sprintf(`
				SELECT
				    %s            AS volume_name,
				    sum(value)    AS s,
				    count()       AS c
				FROM %s
				WHERE %s = @teamID
				  AND %s BETWEEN @start AND @end
				  AND %s = '%s'%s
				GROUP BY volume_name`,
				volAttr,
				TableMetrics,
				ColTeamID, ColTimestamp,
				ColMetricName, metric, nf)
			return r.db.Select(dbutil.OverviewCtx(gctx), dest, query, args...)
		}
	}
	g.Go(runLeg(MetricK8sVolumeCapacity, &capacity))
	g.Go(runLeg(MetricK8sVolumeInodes, &inodes))

	if err := g.Wait(); err != nil {
		return nil, err
	}

	type cell struct {
		capacity, inodes *float64
	}
	cells := make(map[string]*cell)
	keys := make([]string, 0)
	ensure := func(vn string) *cell {
		if c, ok := cells[vn]; ok {
			return c
		}
		c := &cell{}
		cells[vn] = c
		keys = append(keys, vn)
		return c
	}
	for _, row := range capacity {
		c := ensure(row.VolumeName)
		if row.Count > 0 {
			v := row.Sum / float64(row.Count)
			c.capacity = &v
		}
	}
	for _, row := range inodes {
		c := ensure(row.VolumeName)
		if row.Count > 0 {
			v := row.Sum / float64(row.Count)
			c.inodes = &v
		}
	}

	// Mirror the prior ORDER BY capacity_bytes DESC (nil capacity sorts last).
	ordered := make([]string, len(keys))
	copy(ordered, keys)
	capOf := func(name string) float64 {
		c := cells[name]
		if c == nil || c.capacity == nil {
			return -1
		}
		return *c.capacity
	}
	for i := 1; i < len(ordered); i++ {
		j := i
		for j > 0 && capOf(ordered[j-1]) < capOf(ordered[j]) {
			ordered[j-1], ordered[j] = ordered[j], ordered[j-1]
			j--
		}
	}

	out := make([]volumeStatDTO, 0, len(ordered))
	for _, vn := range ordered {
		c := cells[vn]
		stat := VolumeStat{VolumeName: vn}
		if c.capacity != nil {
			stat.CapacityBytes = *c.capacity
		}
		if c.inodes != nil {
			stat.Inodes = int64(*c.inodes)
		}
		out = append(out, stat)
	}
	return out, nil
}

// sortStrings returns a lexicographically-sorted copy of in (stable insertion sort).
func sortStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	for i := 1; i < len(out); i++ {
		j := i
		for j > 0 && out[j-1] > out[j] {
			out[j-1], out[j] = out[j], out[j-1]
			j--
		}
	}
	return out
}

