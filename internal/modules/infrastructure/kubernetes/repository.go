package kubernetes

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
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

// nodeFilter builds the WHERE fragment + named args for the optional node
// filter. Resolves against the rollup's `host` column.
func nodeFilter(node string) (clause string, args []any) {
	if node == "" {
		return "", nil
	}
	return " AND host = @node", []any{clickhouse.Named("node", node)}
}

// bucketExpr returns the bucket column expression. Reads the stored
// ts_bucket directly — no CH-side bucket math; see internal/infra/timebucket.
func bucketExpr(startMs, endMs int64) string {
	_, _ = startMs, endMs
	return "ts_bucket"
}

// queryContainerBuckets reads per-container time-bucketed averages for a given
// metric family. Avg = sum(value_avg_num) / sum(sample_count).
func (r *ClickHouseRepository) queryContainerBuckets(ctx context.Context, teamID int64, startMs, endMs int64, metricName string, node string) ([]containerBucketDTO, error) {
	table := "observability.spans"
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                 AS time_bucket,
		    container                                                          AS container,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName%s
		GROUP BY time_bucket, container
		ORDER BY time_bucket, container
	`, bucketExpr(startMs, endMs), table, nf)

	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("metricName", metricName))
	args = append(args, nfArgs...)
	var rows []containerBucketDTO
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.queryContainerBuckets", &rows, query, args...)
	return rows, err
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
	table := "observability.spans"
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    pod                              AS pod,
		    namespace                        AS namespace,
		    toInt64(max(value_max))     AS restarts
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName%s
		GROUP BY pod, namespace
		ORDER BY restarts DESC
		LIMIT 100
	`, table, nf)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("metricName", MetricK8sContainerRestarts))
	args = append(args, nfArgs...)
	var rows []podStatDTO
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.GetPodRestarts", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetNodeAllocatable(ctx context.Context, teamID int64, startMs, endMs int64, node string) (nodeAllocatableDTO, error) {
	table := "observability.spans"
	nf, nfArgs := nodeFilter(node)

	// Two metric_names in one scan; per-metric avg computed from shared state
	// columns. We group by metric_name and fold client-side — cleaner than
	// -If combinators which Phase-7+ SQL discipline bans.
	query := fmt.Sprintf(`
		SELECT
		    metric_name                                                                AS metric_name,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)     AS avg_val
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN (@cpuMetric, @memMetric)%s
		GROUP BY metric_name
	`, table, nf)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("cpuMetric", MetricK8sNodeAllocatableCPU),
		clickhouse.Named("memMetric", MetricK8sNodeAllocatableMemory),
	)
	args = append(args, nfArgs...)
	var metricRows []struct {
		MetricName string   `ch:"metric_name"`
		AvgVal     *float64 `ch:"avg_val"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.GetNodeAllocatable", &metricRows, query, args...); err != nil {
		return nodeAllocatableDTO{}, err
	}
	var row nodeAllocatableDTO
	for _, mr := range metricRows {
		var val float64
		if mr.AvgVal != nil {
			val = *mr.AvgVal
		}
		switch mr.MetricName {
		case MetricK8sNodeAllocatableCPU:
			row.CPUCores = val
		case MetricK8sNodeAllocatableMemory:
			row.MemoryBytes = val
		}
	}
	return row, nil
}

func (r *ClickHouseRepository) GetPodPhases(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]phaseStatDTO, error) {
	table := "observability.spans"
	nf, nfArgs := nodeFilter(node)

	// Rollup MV extracts `k8s.pod.phase` into `state_dim` for the
	// MetricK8sPodPhase metric_name. sample_count is the per-sample count so
	// we sum it to count points.
	query := fmt.Sprintf(`
		SELECT
		    state_dim                        AS phase,
		    toInt64(sum(sample_count))  AS pod_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName%s
		GROUP BY phase
		ORDER BY pod_count DESC
	`, table, nf)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("metricName", MetricK8sPodPhase))
	args = append(args, nfArgs...)
	var rows []phaseStatDTO
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.GetPodPhases", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetReplicaStatus(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]replicaStatDTO, error) {
	table := "observability.spans"
	nf, nfArgs := nodeFilter(node)

	// state_dim carries the replicaset name for the two relevant metrics.
	// desired / available fold client-side via metric_name.
	query := fmt.Sprintf(`
		SELECT
		    state_dim                                                                  AS replica_set,
		    metric_name                                                                AS metric_name,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)     AS avg_val
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN (@desiredMetric, @availableMetric)%s
		GROUP BY replica_set, metric_name
		ORDER BY replica_set
	`, table, nf)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("desiredMetric", MetricK8sReplicaSetDesired),
		clickhouse.Named("availableMetric", MetricK8sReplicaSetAvailable),
	)
	args = append(args, nfArgs...)
	var metricRows []struct {
		ReplicaSet string   `ch:"replica_set"`
		MetricName string   `ch:"metric_name"`
		AvgVal     *float64 `ch:"avg_val"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.GetReplicaStatus", &metricRows, query, args...); err != nil {
		return nil, err
	}
	out := map[string]*ReplicaStat{}
	for _, mr := range metricRows {
		rs, ok := out[mr.ReplicaSet]
		if !ok {
			rs = &ReplicaStat{ReplicaSet: mr.ReplicaSet}
			out[mr.ReplicaSet] = rs
		}
		var val float64
		if mr.AvgVal != nil {
			val = *mr.AvgVal
		}
		switch mr.MetricName {
		case MetricK8sReplicaSetDesired:
			rs.Desired = int64(val)
		case MetricK8sReplicaSetAvailable:
			rs.Available = int64(val)
		}
	}
	rows := make([]ReplicaStat, 0, len(out))
	for _, rs := range out {
		rows = append(rows, *rs)
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetVolumeUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]volumeStatDTO, error) {
	table := "observability.spans"
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    state_dim                                                                  AS volume_name,
		    metric_name                                                                AS metric_name,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)     AS avg_val
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN (@capMetric, @inodeMetric)%s
		GROUP BY volume_name, metric_name
		ORDER BY volume_name
	`, table, nf)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("capMetric", MetricK8sVolumeCapacity),
		clickhouse.Named("inodeMetric", MetricK8sVolumeInodes),
	)
	args = append(args, nfArgs...)
	var metricRows []struct {
		VolumeName string   `ch:"volume_name"`
		MetricName string   `ch:"metric_name"`
		AvgVal     *float64 `ch:"avg_val"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.GetVolumeUsage", &metricRows, query, args...); err != nil {
		return nil, err
	}
	out := map[string]*VolumeStat{}
	for _, mr := range metricRows {
		v, ok := out[mr.VolumeName]
		if !ok {
			v = &VolumeStat{VolumeName: mr.VolumeName}
			out[mr.VolumeName] = v
		}
		var val float64
		if mr.AvgVal != nil {
			val = *mr.AvgVal
		}
		switch mr.MetricName {
		case MetricK8sVolumeCapacity:
			v.CapacityBytes = val
		case MetricK8sVolumeInodes:
			v.Inodes = int64(val)
		}
	}
	rows := make([]VolumeStat, 0, len(out))
	for _, v := range out {
		rows = append(rows, *v)
	}
	return rows, nil
}
