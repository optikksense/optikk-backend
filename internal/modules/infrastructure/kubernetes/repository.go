package kubernetes

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// Repository defines the data access interface for Kubernetes metrics.
type Repository interface {
	GetContainerCPU(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error)
	GetCPUThrottling(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error)
	GetContainerMemory(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error)
	GetOOMKills(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error)
	GetPodRestarts(teamUUID string, startMs, endMs int64) ([]PodStat, error)
	GetNodeAllocatable(teamUUID string, startMs, endMs int64) (NodeAllocatable, error)
	GetPodPhases(teamUUID string, startMs, endMs int64) ([]PhaseStat, error)
	GetReplicaStatus(teamUUID string, startMs, endMs int64) ([]ReplicaStat, error)
	GetVolumeUsage(teamUUID string, startMs, endMs int64) ([]VolumeStat, error)
}

// ClickHouseRepository implements Repository using ClickHouse.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new ClickHouseRepository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

// queryContainerBuckets is a shared helper for timeseries queries grouped by container name.
func (r *ClickHouseRepository) queryContainerBuckets(teamUUID string, startMs, endMs int64, metricName string) ([]ContainerBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	containerAttr := attrString(AttrContainerName)

	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    %s         AS container,
		    avg(value) AS val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket, container
		ORDER BY time_bucket, container
	`,
		bucket, containerAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetContainerCPU(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error) {
	return r.queryContainerBuckets(teamUUID, startMs, endMs, MetricContainerCPUTime)
}

func (r *ClickHouseRepository) GetCPUThrottling(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error) {
	return r.queryContainerBuckets(teamUUID, startMs, endMs, MetricContainerCPUThrottledTime)
}

func (r *ClickHouseRepository) GetContainerMemory(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error) {
	return r.queryContainerBuckets(teamUUID, startMs, endMs, MetricContainerMemoryUsage)
}

func (r *ClickHouseRepository) GetOOMKills(teamUUID string, startMs, endMs int64) ([]ContainerBucket, error) {
	return r.queryContainerBuckets(teamUUID, startMs, endMs, MetricContainerOOMKillCount)
}

func (r *ClickHouseRepository) GetPodRestarts(teamUUID string, startMs, endMs int64) ([]PodStat, error) {
	podAttr := attrString(AttrK8sPodName)
	nsAttr := attrString(AttrK8sNamespace)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS pod_name,
		    %s              AS namespace,
		    toInt64(max(value)) AS restarts
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY pod_name, namespace
		ORDER BY restarts DESC
		LIMIT 100
	`,
		podAttr, nsAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sContainerRestarts,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetNodeAllocatable(teamUUID string, startMs, endMs int64) (NodeAllocatable, error) {
	query := fmt.Sprintf(`
		SELECT
		    avgIf(value, %s = '%s') AS cpu_cores,
		    avgIf(value, %s = '%s') AS memory_bytes
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')
	`,
		ColMetricName, MetricK8sNodeAllocatableCPU,
		ColMetricName, MetricK8sNodeAllocatableMemory,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sNodeAllocatableCPU, MetricK8sNodeAllocatableMemory,
	)
	row, err := dbutil.QueryMap(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return NodeAllocatable{}, err
	}
	return NodeAllocatable{
		CPUCores:    dbutil.Float64FromAny(row["cpu_cores"]),
		MemoryBytes: dbutil.Float64FromAny(row["memory_bytes"]),
	}, nil
}

func (r *ClickHouseRepository) GetPodPhases(teamUUID string, startMs, endMs int64) ([]PhaseStat, error) {
	phaseAttr := attrString(AttrK8sPodPhase)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS phase,
		    toInt64(count()) AS pod_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY phase
		ORDER BY pod_count DESC
	`,
		phaseAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sPodPhase,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetReplicaStatus(teamUUID string, startMs, endMs int64) ([]ReplicaStat, error) {
	rsAttr := attrString(AttrReplicaSetName)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                         AS replica_set,
		    toInt64(avgIf(value, %s = '%s'))                           AS desired,
		    toInt64(avgIf(value, %s = '%s'))                           AS available
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')
		GROUP BY replica_set
		ORDER BY replica_set
	`,
		rsAttr,
		ColMetricName, MetricK8sReplicaSetDesired,
		ColMetricName, MetricK8sReplicaSetAvailable,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sReplicaSetDesired, MetricK8sReplicaSetAvailable,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetVolumeUsage(teamUUID string, startMs, endMs int64) ([]VolumeStat, error) {
	volAttr := attrString(AttrK8sVolumeName)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                             AS volume_name,
		    avgIf(value, %s = '%s')                                        AS capacity_bytes,
		    toInt64(avgIf(value, %s = '%s'))                               AS inodes
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')
		GROUP BY volume_name
		ORDER BY capacity_bytes DESC
	`,
		volAttr,
		ColMetricName, MetricK8sVolumeCapacity,
		ColMetricName, MetricK8sVolumeInodes,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sVolumeCapacity, MetricK8sVolumeInodes,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
