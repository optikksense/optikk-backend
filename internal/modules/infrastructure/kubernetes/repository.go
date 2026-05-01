package kubernetes

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// All read paths follow the apm/httpmetrics pattern: `WITH active_fps AS
// (... metrics_resource ...)` CTE so the main `observability.metrics` scan
// PREWHEREs on (team_id, ts_bucket, fingerprint). Optional `node` filter
// matches against the real `host` alias column on observability.metrics.

type Repository interface {
	QueryContainerCPUTime(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerRow, error)
	QueryContainerCPUThrottledTime(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerRow, error)
	QueryContainerOOMKills(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerRow, error)
	QueryContainerMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerRow, error)
	QueryPodRestarts(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]PodRestartRow, error)
	QueryNodeAllocatable(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]K8sMetricNameRow, error)
	QueryPodPhases(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]PhaseRow, error)
	QueryReplicaStatus(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ReplicaSetRow, error)
	QueryVolumeUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]VolumeRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

// All container per-metric queries share this shape — same SELECT + filters,
// only the metric_name binding differs. The fold (counter sum vs gauge avg)
// happens service-side, not in SQL.
const containerByMetricQuery = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'k8s.container.name'::String                  AS container,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'k8s.container.name'::String != ''
		  AND (@node = '' OR host = @node)
		ORDER BY timestamp`

func (r *ClickHouseRepository) queryContainerByMetric(ctx context.Context, op, metricName, node string, teamID int64, startMs, endMs int64) ([]ContainerRow, error) {
	args := withMetricNameAndNode(metricArgs(teamID, startMs, endMs), metricName, node)
	var rows []ContainerRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, containerByMetricQuery, args...)
}

func (r *ClickHouseRepository) QueryContainerCPUTime(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerRow, error) {
	return r.queryContainerByMetric(ctx, "kubernetes.QueryContainerCPUTime", MetricContainerCPUTime, node, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryContainerCPUThrottledTime(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerRow, error) {
	return r.queryContainerByMetric(ctx, "kubernetes.QueryContainerCPUThrottledTime", MetricContainerCPUThrottledTime, node, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryContainerOOMKills(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerRow, error) {
	return r.queryContainerByMetric(ctx, "kubernetes.QueryContainerOOMKills", MetricContainerOOMKillCount, node, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryContainerMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ContainerRow, error) {
	return r.queryContainerByMetric(ctx, "kubernetes.QueryContainerMemoryUsage", MetricContainerMemoryUsage, node, teamID, startMs, endMs)
}

// QueryPodRestarts returns per-(pod, namespace) max restart count.
func (r *ClickHouseRepository) QueryPodRestarts(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]PodRestartRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    attributes.'k8s.pod.name'::String                        AS pod,
		    attributes.'k8s.namespace.name'::String                  AS namespace,
		    toInt64(max(value))                                      AS restarts
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'k8s.pod.name'::String != ''
		  AND (@node = '' OR host = @node)
		GROUP BY pod, namespace
		ORDER BY restarts DESC
		LIMIT @podRestartLimit`
	args := withMetricNameAndNode(metricArgs(teamID, startMs, endMs), MetricK8sContainerRestarts, node)
	args = append(args, clickhouse.Named("podRestartLimit", uint64(100)))
	var rows []PodRestartRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.QueryPodRestarts", &rows, query, args...)
}

// QueryNodeAllocatable returns one row per node-allocatable metric (cpu+memory).
func (r *ClickHouseRepository) QueryNodeAllocatable(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]K8sMetricNameRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    metric_name AS metric_name,
		    sum(val_sum) / sum(val_count)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND (@node = '' OR host = @node)
		GROUP BY metric_name`
	args := withMetricNamesAndNode(metricArgs(teamID, startMs, endMs), []string{
		MetricK8sNodeAllocatableCPU,
		MetricK8sNodeAllocatableMemory,
	}, node)
	var rows []K8sMetricNameRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.QueryNodeAllocatable", &rows, query, args...)
}

// QueryPodPhases returns one row per pod phase (Running/Pending/etc.) with the
// count of pods in that phase (sum of data points).
func (r *ClickHouseRepository) QueryPodPhases(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]PhaseRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    attributes.'k8s.pod.phase'::String  AS phase,
		    toInt64(count())                    AS pod_count
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'k8s.pod.phase'::String != ''
		  AND (@node = '' OR host = @node)
		GROUP BY phase
		ORDER BY pod_count DESC`
	args := withMetricNameAndNode(metricArgs(teamID, startMs, endMs), MetricK8sPodPhase, node)
	var rows []PhaseRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.QueryPodPhases", &rows, query, args...)
}

// QueryReplicaStatus returns per-(replicaset, metric_name) values for
// desired+available pair.
func (r *ClickHouseRepository) QueryReplicaStatus(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]ReplicaSetRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'k8s.replicaset.name'::String                 AS replica_set,
		    metric_name                                              AS metric_name,
		    sum(val_sum) / sum(val_count)                                               AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'k8s.replicaset.name'::String != ''
		  AND (@node = '' OR host = @node)
		GROUP BY replica_set, metric_name
		ORDER BY replica_set`
	args := withMetricNamesAndNode(metricArgs(teamID, startMs, endMs), []string{
		MetricK8sReplicaSetDesired,
		MetricK8sReplicaSetAvailable,
	}, node)
	var rows []ReplicaSetRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.QueryReplicaStatus", &rows, query, args...)
}

// QueryVolumeUsage returns per-(volume, metric_name) values for capacity+inodes pair.
func (r *ClickHouseRepository) QueryVolumeUsage(ctx context.Context, teamID int64, startMs, endMs int64, node string) ([]VolumeRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'k8s.volume.name'::String                     AS volume_name,
		    metric_name                                              AS metric_name,
		    sum(val_sum) / sum(val_count)                                               AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'k8s.volume.name'::String != ''
		  AND (@node = '' OR host = @node)
		GROUP BY volume_name, metric_name
		ORDER BY volume_name`
	args := withMetricNamesAndNode(metricArgs(teamID, startMs, endMs), []string{
		MetricK8sVolumeCapacity,
		MetricK8sVolumeInodes,
	}, node)
	var rows []VolumeRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kubernetes.QueryVolumeUsage", &rows, query, args...)
}

// ---------------------------------------------------------------------------
// Local helpers — each module owns its own.
// ---------------------------------------------------------------------------

func metricArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func metricBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

func withMetricNameAndNode(args []any, metricName, node string) []any {
	args = append(args,
		clickhouse.Named("metricName", metricName),
		clickhouse.Named("node", node),
	)
	return args
}

func withMetricNamesAndNode(args []any, metricNames []string, node string) []any {
	args = append(args,
		clickhouse.Named("metricNames", metricNames),
		clickhouse.Named("node", node),
	)
	return args
}
