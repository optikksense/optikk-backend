package kubernetes

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetContainerCPU(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error)
	GetCPUThrottling(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error)
	GetContainerMemory(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error)
	GetOOMKills(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error)
	GetPodRestarts(teamID int64, startMs, endMs int64, node string) ([]PodStat, error)
	GetNodeAllocatable(teamID int64, startMs, endMs int64, node string) (NodeAllocatable, error)
	GetPodPhases(teamID int64, startMs, endMs int64, node string) ([]PhaseStat, error)
	GetReplicaStatus(teamID int64, startMs, endMs int64, node string) ([]ReplicaStat, error)
	GetVolumeUsage(teamID int64, startMs, endMs int64, node string) ([]VolumeStat, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

func nodeFilter(node string) (string, []any) {
	if node == "" {
		return "", nil
	}
	return " AND attributes.'k8s.node.name'::String = ?", []any{node}
}

func (r *ClickHouseRepository) queryContainerBuckets(teamID int64, startMs, endMs int64, metricName string, node string) ([]ContainerBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	containerAttr := attrString(AttrContainerName)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    %s         AS container,
		    avg(value) AS val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'%s
		GROUP BY time_bucket, container
		ORDER BY time_bucket, container
	`,
		bucket, containerAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, metricName, nf,
	)
	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, nfArgs...)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	results := make([]ContainerBucket, len(rows))
	for i, row := range rows {
		v := dbutil.NullableFloat64FromAny(row["val"])
		results[i] = ContainerBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Container: dbutil.StringFromAny(row["container"]),
			Value:     v,
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetContainerCPU(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return r.queryContainerBuckets(teamID, startMs, endMs, MetricContainerCPUTime, node)
}

func (r *ClickHouseRepository) GetCPUThrottling(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return r.queryContainerBuckets(teamID, startMs, endMs, MetricContainerCPUThrottledTime, node)
}

func (r *ClickHouseRepository) GetContainerMemory(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return r.queryContainerBuckets(teamID, startMs, endMs, MetricContainerMemoryUsage, node)
}

func (r *ClickHouseRepository) GetOOMKills(teamID int64, startMs, endMs int64, node string) ([]ContainerBucket, error) {
	return r.queryContainerBuckets(teamID, startMs, endMs, MetricContainerOOMKillCount, node)
}

func (r *ClickHouseRepository) GetPodRestarts(teamID int64, startMs, endMs int64, node string) ([]PodStat, error) {
	podAttr := attrString(AttrK8sPodName)
	nsAttr := attrString(AttrK8sNamespace)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS pod_name,
		    %s              AS namespace,
		    toInt64(max(value)) AS restarts
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
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
	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, nfArgs...)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	results := make([]PodStat, len(rows))
	for i, row := range rows {
		results[i] = PodStat{
			PodName:   dbutil.StringFromAny(row["pod_name"]),
			Namespace: dbutil.StringFromAny(row["namespace"]),
			Restarts:  dbutil.Int64FromAny(row["restarts"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetNodeAllocatable(teamID int64, startMs, endMs int64, node string) (NodeAllocatable, error) {
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    avgIf(value, %s = '%s') AS cpu_cores,
		    avgIf(value, %s = '%s') AS memory_bytes
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')%s
	`,
		ColMetricName, MetricK8sNodeAllocatableCPU,
		ColMetricName, MetricK8sNodeAllocatableMemory,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sNodeAllocatableCPU, MetricK8sNodeAllocatableMemory, nf,
	)
	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, nfArgs...)
	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil {
		return NodeAllocatable{}, err
	}
	return NodeAllocatable{
		CPUCores:    dbutil.Float64FromAny(row["cpu_cores"]),
		MemoryBytes: dbutil.Float64FromAny(row["memory_bytes"]),
	}, nil
}

func (r *ClickHouseRepository) GetPodPhases(teamID int64, startMs, endMs int64, node string) ([]PhaseStat, error) {
	phaseAttr := attrString(AttrK8sPodPhase)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS phase,
		    toInt64(count()) AS pod_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'%s
		GROUP BY phase
		ORDER BY pod_count DESC
	`,
		phaseAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sPodPhase, nf,
	)
	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, nfArgs...)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	results := make([]PhaseStat, len(rows))
	for i, row := range rows {
		results[i] = PhaseStat{
			Phase: dbutil.StringFromAny(row["phase"]),
			Count: dbutil.Int64FromAny(row["pod_count"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetReplicaStatus(teamID int64, startMs, endMs int64, node string) ([]ReplicaStat, error) {
	rsAttr := attrString(AttrReplicaSetName)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                         AS replica_set,
		    toInt64(avgIf(value, %s = '%s'))                           AS desired,
		    toInt64(avgIf(value, %s = '%s'))                           AS available
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')%s
		GROUP BY replica_set
		ORDER BY replica_set
	`,
		rsAttr,
		ColMetricName, MetricK8sReplicaSetDesired,
		ColMetricName, MetricK8sReplicaSetAvailable,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sReplicaSetDesired, MetricK8sReplicaSetAvailable, nf,
	)
	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, nfArgs...)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	results := make([]ReplicaStat, len(rows))
	for i, row := range rows {
		results[i] = ReplicaStat{
			ReplicaSet: dbutil.StringFromAny(row["replica_set"]),
			Desired:    dbutil.Int64FromAny(row["desired"]),
			Available:  dbutil.Int64FromAny(row["available"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetVolumeUsage(teamID int64, startMs, endMs int64, node string) ([]VolumeStat, error) {
	volAttr := attrString(AttrK8sVolumeName)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                             AS volume_name,
		    avgIf(value, %s = '%s')                                        AS capacity_bytes,
		    toInt64(avgIf(value, %s = '%s'))                               AS inodes
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')%s
		GROUP BY volume_name
		ORDER BY capacity_bytes DESC
	`,
		volAttr,
		ColMetricName, MetricK8sVolumeCapacity,
		ColMetricName, MetricK8sVolumeInodes,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sVolumeCapacity, MetricK8sVolumeInodes, nf,
	)
	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, nfArgs...)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	results := make([]VolumeStat, len(rows))
	for i, row := range rows {
		results[i] = VolumeStat{
			VolumeName:    dbutil.StringFromAny(row["volume_name"]),
			CapacityBytes: dbutil.Float64FromAny(row["capacity_bytes"]),
			Inodes:        dbutil.Int64FromAny(row["inodes"]),
		}
	}
	return results, nil
}
