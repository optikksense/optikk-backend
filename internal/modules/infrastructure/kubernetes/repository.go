package kubernetes

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
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
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) Repository {
	return &ClickHouseRepository{db: db}
}

func nodeFilter(node string) (clause string, args []any) {
	if node == "" {
		return "", nil
	}
	return " AND attributes.'k8s.node.name'::String = @node", []any{clickhouse.Named("node", node)}
}

func (r *ClickHouseRepository) queryContainerBuckets(ctx context.Context, teamID int64, startMs, endMs int64, metricName string, node string) ([]containerBucketDTO, error) {
	bucket := timebucket.Expression(startMs, endMs)
	containerAttr := attrString(AttrContainerName)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    %s         AS container,
		    avg(value) AS val
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'%s
		GROUP BY time_bucket, container
		ORDER BY time_bucket, container
	`,
		bucket, containerAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, metricName, nf,
	)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)
	var rows []containerBucketDTO
	err := r.db.Select(ctx, &rows, query, args...)
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
	podAttr := attrString(AttrK8sPodName)
	nsAttr := attrString(AttrK8sNamespace)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS pod_name,
		    %s              AS namespace,
		    toInt64(max(value)) AS restarts
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
	var rows []podStatDTO
	err := r.db.Select(ctx, &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetNodeAllocatable(ctx context.Context, teamID int64, startMs, endMs int64, node string) (nodeAllocatableDTO, error) {
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    avgIf(value, %s = '%s') AS cpu_cores,
		    avgIf(value, %s = '%s') AS memory_bytes
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')%s
	`,
		ColMetricName, MetricK8sNodeAllocatableCPU,
		ColMetricName, MetricK8sNodeAllocatableMemory,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sNodeAllocatableCPU, MetricK8sNodeAllocatableMemory, nf,
	)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)
	var row nodeAllocatableDTO
	err := r.db.QueryRow(ctx, &row, query, args...)
	return row, err
}

func (r *ClickHouseRepository) GetPodPhases(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]phaseStatDTO, error) {
	phaseAttr := attrString(AttrK8sPodPhase)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s              AS phase,
		    toInt64(count()) AS pod_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'%s
		GROUP BY phase
		ORDER BY pod_count DESC
	`,
		phaseAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricK8sPodPhase, nf,
	)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)
	var rows []phaseStatDTO
	err := r.db.Select(ctx, &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetReplicaStatus(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]replicaStatDTO, error) {
	rsAttr := attrString(AttrReplicaSetName)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                         AS replica_set,
		    toInt64(avgIf(value, %s = '%s'))                           AS desired,
		    toInt64(avgIf(value, %s = '%s'))                           AS available
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
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
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)
	var rows []ReplicaStat
	err := r.db.Select(ctx, &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetVolumeUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]volumeStatDTO, error) {
	volAttr := attrString(AttrK8sVolumeName)
	nf, nfArgs := nodeFilter(node)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                             AS volume_name,
		    avgIf(value, %s = '%s')                                        AS capacity_bytes,
		    toInt64(avgIf(value, %s = '%s'))                               AS inodes
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
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
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs), nfArgs...)
	var rows []VolumeStat
	err := r.db.Select(ctx, &rows, query, args...)
	return rows, err
}
