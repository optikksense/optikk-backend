package explorer

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/filter"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func buildFilterArgs(teamID, startMs, endMs int64, metricNames []string, filterCol, filterVal string) (string, []any) {
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), metricNames)
	var extraWhere string
	if filterVal != "" {
		if filterCol == "topic" {
			extraWhere = "AND attributes.'messaging.destination.name'::String = @filterVal"
		} else if filterCol == "consumer_group" {
			extraWhere = "AND attributes.'messaging.consumer.group.name'::String = @filterVal"
		}
		args = append(args, clickhouse.Named("filterVal", filterVal))
	}
	return extraWhere, args
}

var topicThroughputMetrics = []string{
	"kafka.consumer.bytes_consumed_rate",
	"kafka.consumer.bytes_consumed_total",
	"kafka.consumer.records_consumed_rate",
	"kafka.consumer.records_consumed_total",
}

func (r *Repository) QueryTopicThroughput(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicThroughputRow, error) {
	extraWhere, args := buildFilterArgs(teamID, startMs, endMs, topicThroughputMetrics, "topic", topic)
	query := fmt.Sprintf(`
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.destination.name'::String AS topic,
		    avg(if(metric_name = 'kafka.consumer.bytes_consumed_rate',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS bytes_per_sec,
		    max(if(metric_name = 'kafka.consumer.bytes_consumed_total',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS bytes_total,
		    avg(if(metric_name = 'kafka.consumer.records_consumed_rate',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS records_per_sec,
		    max(if(metric_name = 'kafka.consumer.records_consumed_total',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS records_total
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  %s
		GROUP BY topic
		HAVING topic != ''
		ORDER BY bytes_per_sec DESC, topic ASC`, extraWhere)
	rows := make([]TopicThroughputRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryTopicThroughput", &rows, query, args...)
}

var topicLagMetrics = []string{
	"kafka.consumer.records_lag",
	"kafka.consumer.records_lead",
}

func (r *Repository) QueryTopicLag(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicLagRow, error) {
	extraWhere, args := buildFilterArgs(teamID, startMs, endMs, topicLagMetrics, "topic", topic)
	query := fmt.Sprintf(`
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.destination.name'::String AS topic,
		    max(if(metric_name = 'kafka.consumer.records_lag',
		           ifNotFinite(val_sum / val_count, 0), NULL))  AS lag,
		    max(if(metric_name = 'kafka.consumer.records_lead',
		           ifNotFinite(val_sum / val_count, 0), NULL))  AS lead
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  %s
		GROUP BY topic
		HAVING topic != ''
		ORDER BY lag DESC, topic ASC`, extraWhere)
	rows := make([]TopicLagRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryTopicLag", &rows, query, args...)
}

func (r *Repository) QueryTopicConsumers(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicConsumersRow, error) {
	extraWhere, args := buildFilterArgs(teamID, startMs, endMs, topicLagMetrics, "topic", topic)
	query := fmt.Sprintf(`
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.destination.name'::String AS topic,
		    count(DISTINCT attributes.'messaging.consumer.group.name'::String) AS consumer_group_count
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  %s
		GROUP BY topic
		ORDER BY topic ASC`, extraWhere)
	rows := make([]TopicConsumersRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryTopicConsumers", &rows, query, args...)
}

var groupPartitionMetrics = []string{"kafka.consumer.assigned_partitions"}

func (r *Repository) QueryGroupPartitions(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupPartitionsRow, error) {
	extraWhere, args := buildFilterArgs(teamID, startMs, endMs, groupPartitionMetrics, "consumer_group", group)
	query := fmt.Sprintf(`
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.consumer.group.name'::String AS consumer_group,
		    max(ifNotFinite(val_sum / val_count, 0))            AS assigned_partitions
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		  %s
		GROUP BY consumer_group
		HAVING consumer_group != ''
		ORDER BY assigned_partitions DESC, consumer_group ASC`, extraWhere)
	rows := make([]GroupPartitionsRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryGroupPartitions", &rows, query, args...)
}

var groupCommitMetrics = []string{
	"kafka.consumer.commit_rate",
	"kafka.consumer.commit_latency_avg",
	"kafka.consumer.commit_latency_max",
}

func (r *Repository) QueryGroupCommits(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupCommitsRow, error) {
	extraWhere, args := buildFilterArgs(teamID, startMs, endMs, groupCommitMetrics, "consumer_group", group)
	query := fmt.Sprintf(`
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.consumer.group.name'::String AS consumer_group,
		    avg(if(metric_name = 'kafka.consumer.commit_rate',
		           ifNotFinite(val_sum / val_count, 0), NULL))        AS commit_rate,
		    ifNotFinite(avg(if(metric_name = 'kafka.consumer.commit_latency_avg',
		           ifNotFinite(val_sum / val_count, 0), NULL)), 0)    AS commit_latency_avg_ms,
		    max(if(metric_name = 'kafka.consumer.commit_latency_max',
		           ifNotFinite(val_sum / val_count, 0), NULL))        AS commit_latency_max_ms
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		  %s
		GROUP BY consumer_group
		HAVING consumer_group != ''
		ORDER BY consumer_group ASC`, extraWhere)
	rows := make([]GroupCommitsRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryGroupCommits", &rows, query, args...)
}

var groupFetchMetrics = []string{
	"kafka.consumer.fetch_rate",
	"kafka.consumer.fetch_latency_avg",
	"kafka.consumer.fetch_latency_max",
}

func (r *Repository) QueryGroupFetches(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupFetchesRow, error) {
	extraWhere, args := buildFilterArgs(teamID, startMs, endMs, groupFetchMetrics, "consumer_group", group)
	query := fmt.Sprintf(`
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.consumer.group.name'::String AS consumer_group,
		    avg(if(metric_name = 'kafka.consumer.fetch_rate',
		           ifNotFinite(val_sum / val_count, 0), NULL))        AS fetch_rate,
		    ifNotFinite(avg(if(metric_name = 'kafka.consumer.fetch_latency_avg',
		           ifNotFinite(val_sum / val_count, 0), NULL)), 0)    AS fetch_latency_avg_ms,
		    max(if(metric_name = 'kafka.consumer.fetch_latency_max',
		           ifNotFinite(val_sum / val_count, 0), NULL))        AS fetch_latency_max_ms
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		  %s
		GROUP BY consumer_group
		HAVING consumer_group != ''
		ORDER BY consumer_group ASC`, extraWhere)
	rows := make([]GroupFetchesRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryGroupFetches", &rows, query, args...)
}

var groupHealthMetrics = []string{
	"kafka.consumer.heartbeat_rate",
	"kafka.consumer.failed_rebalance_rate_per_hour",
	"kafka.consumer.poll_idle_ratio_avg",
	"kafka.consumer.last_poll_seconds_ago",
	"kafka.consumer.connection_count",
}

func (r *Repository) QueryGroupHealth(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupHealthRow, error) {
	extraWhere, args := buildFilterArgs(teamID, startMs, endMs, groupHealthMetrics, "consumer_group", group)
	query := fmt.Sprintf(`
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.consumer.group.name'::String AS consumer_group,
		    avg(if(metric_name = 'kafka.consumer.heartbeat_rate',
		           ifNotFinite(val_sum / val_count, 0), NULL))                    AS heartbeat_rate,
		    avg(if(metric_name = 'kafka.consumer.failed_rebalance_rate_per_hour',
		           ifNotFinite(val_sum / val_count, 0), NULL))                    AS failed_rebalance_per_hour,
		    ifNotFinite(avg(if(metric_name = 'kafka.consumer.poll_idle_ratio_avg',
		           ifNotFinite(val_sum / val_count, 0), NULL)), 0)                AS poll_idle_ratio,
		    max(if(metric_name = 'kafka.consumer.last_poll_seconds_ago',
		           ifNotFinite(val_sum / val_count, 0), NULL))                    AS last_poll_seconds_ago,
		    max(if(metric_name = 'kafka.consumer.connection_count',
		           ifNotFinite(val_sum / val_count, 0), NULL))                    AS connection_count
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		  %s
		GROUP BY consumer_group
		HAVING consumer_group != ''
		ORDER BY consumer_group ASC`, extraWhere)
	rows := make([]GroupHealthRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryGroupHealth", &rows, query, args...)
}

func (r *Repository) QueryTopicGroupThroughput(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicGroupThroughputRow, error) {
	extraWhere, args := buildFilterArgs(teamID, startMs, endMs, topicThroughputMetrics, "topic", topic)
	query := fmt.Sprintf(`
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.consumer.group.name'::String AS consumer_group,
		    avg(if(metric_name = 'kafka.consumer.bytes_consumed_rate',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS bytes_per_sec,
		    avg(if(metric_name = 'kafka.consumer.records_consumed_rate',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS records_per_sec
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  AND attributes.'messaging.consumer.group.name'::String != ''
		  %s
		GROUP BY consumer_group
		HAVING consumer_group != ''
		ORDER BY consumer_group ASC`, extraWhere)
	rows := make([]TopicGroupThroughputRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryTopicGroupThroughput", &rows, query, args...)
}

func (r *Repository) QueryTopicGroupLag(ctx context.Context, teamID, startMs, endMs int64, topic string) ([]TopicGroupLagRow, error) {
	extraWhere, args := buildFilterArgs(teamID, startMs, endMs, topicLagMetrics, "topic", topic)
	query := fmt.Sprintf(`
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.consumer.group.name'::String AS consumer_group,
		    max(if(metric_name = 'kafka.consumer.records_lag',
		           ifNotFinite(val_sum / val_count, 0), NULL))  AS lag,
		    max(if(metric_name = 'kafka.consumer.records_lead',
		           ifNotFinite(val_sum / val_count, 0), NULL))  AS lead
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  AND attributes.'messaging.consumer.group.name'::String != ''
		  %s
		GROUP BY consumer_group
		HAVING consumer_group != ''
		ORDER BY lag DESC`, extraWhere)
	rows := make([]TopicGroupLagRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryTopicGroupLag", &rows, query, args...)
}

var groupTopicMetrics = []string{
	"kafka.consumer.bytes_consumed_rate",
	"kafka.consumer.bytes_consumed_total",
	"kafka.consumer.records_consumed_rate",
	"kafka.consumer.records_consumed_total",
	"kafka.consumer.records_lag",
	"kafka.consumer.records_lead",
}

func (r *Repository) QueryGroupTopics(ctx context.Context, teamID, startMs, endMs int64, group string) ([]GroupTopicRow, error) {
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), groupTopicMetrics)
	args = append(args, clickhouse.Named("filterVal", group))
	query := `
		WITH active_fps AS (
		    SELECT fingerprint FROM observability.metrics_resource
		    WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.destination.name'::String AS topic,
		    avg(if(metric_name = 'kafka.consumer.bytes_consumed_rate',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS bytes_per_sec,
		    max(if(metric_name = 'kafka.consumer.bytes_consumed_total',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS bytes_total,
		    avg(if(metric_name = 'kafka.consumer.records_consumed_rate',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS records_per_sec,
		    max(if(metric_name = 'kafka.consumer.records_consumed_total',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS records_total,
		    max(if(metric_name = 'kafka.consumer.records_lag',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS lag,
		    max(if(metric_name = 'kafka.consumer.records_lead',
		           ifNotFinite(val_sum / val_count, 0), NULL))    AS lead
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  AND attributes.'messaging.consumer.group.name'::String = @filterVal
		GROUP BY topic
		HAVING topic != ''
		ORDER BY bytes_per_sec DESC, topic ASC`
	rows := make([]GroupTopicRow, 0)
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryGroupTopics", &rows, query, args...)
}
