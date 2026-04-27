package kafka

import (
	"context"
	"fmt"
	"math"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// rollupBucketExpr returns the rollup bucket column expression. Reads stored
// ts_bucket directly — no CH-side bucket math; see internal/infra/timebucket.
func rollupBucketExpr(startMs, endMs int64) string {
	_, _ = startMs, endMs
	return "ts_bucket"
}

// bucketSecs returns the bucket width in seconds matching the adaptive strategy.
func bucketSecs(startMs, endMs int64) float64 {
	durationSecs := (endMs - startMs) / 1000
	if durationSecs <= 3600 {
		return 60.0
	} else if durationSecs <= 86400 {
		return 300.0
	}
	return 3600.0
}

func (r *ClickHouseRepository) GetKafkaSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) (KafkaSummaryStats, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}
	var stats KafkaSummaryStats

	// Single query against the narrow kafka_summary rollup (3 dims vs 11).
	// Uses conditional merge functions to compute all summary fields at once.
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    toFloat64(sumMergeIf(value_sum, metric_name IN @producerMetrics))       AS prod_sum,
		    toFloat64(sumMergeIf(value_sum, metric_name IN @consumerMetrics))       AS cons_sum,
		    toFloat64(maxMergeIf(value_max, metric_name IN @lagMetrics))            AS max_lag,
		    toFloat64(quantilesTDigestWeightedMergeIf(0.5, 0.95, 0.99)(latency_digest, metric_name = @publishDuration)[2])  AS publish_p95,
		    toFloat64(quantilesTDigestWeightedMergeIf(0.5, 0.95, 0.99)(latency_digest, metric_name = @receiveDuration)[2])  AS receive_p95
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
	`, table)

	var row struct {
		ProdSum    float64 `ch:"prod_sum"`
		ConsSum    float64 `ch:"cons_sum"`
		MaxLag     float64 `ch:"max_lag"`
		PublishP95 float64 `ch:"publish_p95"`
		ReceiveP95 float64 `ch:"receive_p95"`
	}
	args := append(r.rollupBaseParams(teamID, startMs, endMs),
		clickhouse.Named("producerMetrics", ProducerMetrics),
		clickhouse.Named("consumerMetrics", ConsumerMetrics),
		clickhouse.Named("lagMetrics", ConsumerLagMetrics),
		clickhouse.Named("publishDuration", MetricPublishDuration),
		clickhouse.Named("receiveDuration", MetricReceiveDuration),
	)
	if err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetKafkaSummaryStats", &row, query, args...); err != nil {
		return stats, err
	}

	stats.PublishRatePerSec = row.ProdSum / durationSecs
	stats.ReceiveRatePerSec = row.ConsSum / durationSecs
	stats.MaxLag = row.MaxLag
	stats.PublishP95Ms = row.PublishP95
	stats.ReceiveP95Ms = row.ReceiveP95
	return stats, nil
}

// ---------------------------------------------------------------------------
// Rate panels — messaging_counters_rollup, sum(value_sum) / bucketSecs
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetProduceRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	return r.topicRate(ctx, teamID, startMs, endMs, f, ProducerMetrics)
}

func (r *ClickHouseRepository) GetConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	return r.topicRate(ctx, teamID, startMs, endMs, f, ConsumerMetrics)
}

func (r *ClickHouseRepository) topicRate(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metrics []string) ([]TopicRatePoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                       AS time_bucket,
		    messaging_destination                    AS topic,
		    toFloat64(sum(value_sum)) / @bucketSecs AS rate_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metrics
		  %s
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("bucketSecs", bucketSecs(startMs, endMs)), clickhouse.Named("metrics", metrics))
	var out []TopicRatePoint
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.topicRate", &out, query, args...)
}

func (r *ClickHouseRepository) GetConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	return r.groupRate(ctx, teamID, startMs, endMs, f, ConsumerMetrics)
}

func (r *ClickHouseRepository) GetProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	return r.groupRate(ctx, teamID, startMs, endMs, f, ProcessMetrics)
}

func (r *ClickHouseRepository) groupRate(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metrics []string) ([]GroupRatePoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                           AS time_bucket,
		    consumer_group                               AS consumer_group,
		    toFloat64(sum(value_sum)) / @bucketSecs AS rate_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metrics
		  %s
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("bucketSecs", bucketSecs(startMs, endMs)), clickhouse.Named("metrics", metrics))
	var out []GroupRatePoint
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.groupRate", &out, query, args...)
}

// ---------------------------------------------------------------------------
// Lag — messaging_counters_rollup; avg via value_avg_num / sample_count
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetConsumerLagByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]LagPoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    consumer_group                                                              AS consumer_group,
		    messaging_destination                                                       AS topic,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)      AS lag
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @lagMetrics
		  %s
		GROUP BY time_bucket, consumer_group, topic
		ORDER BY time_bucket ASC, consumer_group ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("lagMetrics", ConsumerLagMetrics))
	var out []LagPoint
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetConsumerLagByGroup", &out, query, args...)
}

func (r *ClickHouseRepository) GetConsumerLagPerPartition(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]PartitionLag, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    messaging_destination                                                       AS topic,
		    toInt64OrZero(partition)                                                    AS partition,
		    consumer_group                                                              AS consumer_group,
		    toInt64(sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)) AS lag
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @lagMetrics
		  %s
		GROUP BY topic, partition, consumer_group
		ORDER BY lag DESC
		LIMIT 200
	`, table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("lagMetrics", ConsumerLagMetrics))
	var out []PartitionLag
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetConsumerLagPerPartition", &out, query, args...)
}

// ---------------------------------------------------------------------------
// Rebalance — messaging_counters_rollup; 6 metric_names, fold client-side
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]RebalancePoint, error) {
	table := "observability.spans"
	bs := bucketSecs(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    consumer_group                                                              AS consumer_group,
		    metric_name                                                                 AS metric_name,
		    toFloat64(sum(value_sum))                                              AS v_sum,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)      AS v_avg
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @rebalanceMetrics
		  %s
		GROUP BY time_bucket, consumer_group, metric_name
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("rebalanceMetrics", RebalanceMetrics))
	var metricRows []struct {
		TimeBucket    string  `ch:"time_bucket"`
		ConsumerGroup string  `ch:"consumer_group"`
		MetricName    string  `ch:"metric_name"`
		VSum          float64 `ch:"v_sum"`
		VAvg          float64 `ch:"v_avg"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetRebalanceSignals", &metricRows, query, args...); err != nil {
		return nil, err
	}
	type key struct{ bucket, group string }
	out := map[key]*RebalancePoint{}
	for _, mr := range metricRows {
		k := key{mr.TimeBucket, mr.ConsumerGroup}
		pt, ok := out[k]
		if !ok {
			pt = &RebalancePoint{Timestamp: k.bucket, ConsumerGroup: k.group}
			out[k] = pt
		}
		switch mr.MetricName {
		case MetricRebalanceCount:
			pt.RebalanceRate = mr.VSum / bs
		case MetricJoinCount:
			pt.JoinRate = mr.VSum / bs
		case MetricSyncCount:
			pt.SyncRate = mr.VSum / bs
		case MetricHeartbeatCount:
			pt.HeartbeatRate = mr.VSum / bs
		case MetricFailedHeartbeatCount:
			pt.FailedHeartbeatRate = mr.VSum / bs
		case MetricAssignedPartitions:
			pt.AssignedPartitions = mr.VAvg
		}
	}
	rows := make([]RebalancePoint, 0, len(out))
	for _, pt := range out {
		rows = append(rows, *pt)
	}
	return rows, nil
}

// ---------------------------------------------------------------------------
// E2E latency — messaging_histograms_rollup; 3 metric_names, fold client-side
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetE2ELatency(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]E2ELatencyPoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    messaging_destination                                                AS topic,
		    metric_name                                                          AS metric_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])  AS p95
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN (@publishDuration, @receiveDuration, @processDuration)
		  %s
		GROUP BY time_bucket, topic, metric_name
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	var metricRows []struct {
		TimeBucket string  `ch:"time_bucket"`
		Topic      string  `ch:"topic"`
		MetricName string  `ch:"metric_name"`
		P95        float64 `ch:"p95"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetE2ELatency", &metricRows, query, args...); err != nil {
		return nil, err
	}
	type key struct{ bucket, topic string }
	out := map[key]*E2ELatencyPoint{}
	for _, mr := range metricRows {
		k := key{mr.TimeBucket, mr.Topic}
		pt, ok := out[k]
		if !ok {
			pt = &E2ELatencyPoint{Timestamp: k.bucket, Topic: k.topic}
			out[k] = pt
		}
		switch mr.MetricName {
		case MetricPublishDuration:
			pt.PublishP95Ms = mr.P95
		case MetricReceiveDuration:
			pt.ReceiveP95Ms = mr.P95
		case MetricProcessDuration:
			pt.ProcessP95Ms = mr.P95
		}
	}
	rows := make([]E2ELatencyPoint, 0, len(out))
	for _, pt := range out {
		rows = append(rows, *pt)
	}
	return rows, nil
}

// ---------------------------------------------------------------------------
// Error panels — messaging_counters_rollup, filter on error_type != ''
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetPublishErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return r.getTopicErrorRates(ctx, teamID, startMs, endMs, MetricPublishMessages, "GetPublishErrors", f)
}

func (r *ClickHouseRepository) GetConsumeErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return r.getGroupErrorRates(ctx, teamID, startMs, endMs, MetricReceiveMessages, "GetConsumeErrors", f)
}

func (r *ClickHouseRepository) GetProcessErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return r.getGroupErrorRates(ctx, teamID, startMs, endMs, MetricProcessMessages, "GetProcessErrors", f)
}

// GetClientOpErrors groups by (operation, error_type) for the
// `messaging.client.operation.duration` metric. It's a histogram, but
// messaging_counters_rollup carries its error-tagged rows via `sample_count`
// (see MV in db/clickhouse/14_rollup_messaging_counters.sql). Rate is
// errored-ops / bucketSecs.
func (r *ClickHouseRepository) GetClientOpErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                              AS time_bucket,
		    messaging_operation                                             AS operation_name,
		    error_type                                                      AS error_type,
		    toFloat64(sum(sample_count)) / @bucketSecs                 AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @opDuration
		  AND error_type != ''
		  %s
		GROUP BY time_bucket, operation_name, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("bucketSecs", bucketSecs(startMs, endMs)))
	var out []ErrorRatePoint
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetClientOpErrors", &out, query, args...)
}

func (r *ClickHouseRepository) GetBrokerConnections(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]BrokerConnectionPoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    broker                                                                      AS broker,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)      AS connections
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, broker
		ORDER BY time_bucket ASC, broker ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("metricName", MetricClientConnections))
	var out []BrokerConnectionPoint
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetBrokerConnections", &out, query, args...)
}

// ---------------------------------------------------------------------------
// Histogram-latency panels — messaging_histograms_rollup (Phase 7)
// ---------------------------------------------------------------------------

// GetPublishLatencyByTopic reads publish-duration percentiles from
// `messaging_histograms_rollup_*` grouped by topic (= `messaging_destination`).
func (r *ClickHouseRepository) GetPublishLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	return r.topicLatency(ctx, teamID, startMs, endMs, f, MetricPublishDuration, publishOperationAliases)
}

// GetReceiveLatencyByTopic — rollup-backed, see comment on GetPublishLatencyByTopic.
func (r *ClickHouseRepository) GetReceiveLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	return r.topicLatency(ctx, teamID, startMs, endMs, f, MetricReceiveDuration, receiveOperationAliases)
}

func (r *ClickHouseRepository) topicLatency(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, durationMetric string, opAliases []string) ([]TopicLatencyPoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    messaging_destination                                                AS topic,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1])  AS p50,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])  AS p95,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3])  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND (metric_name = @durationMetric
		       OR (metric_name = @opDuration AND lower(messaging_operation) IN @opAliases))
		  %s
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args,
		clickhouse.Named("durationMetric", durationMetric),
		clickhouse.Named("opAliases", opAliases),
	)
	var out []TopicLatencyPoint
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.topicLatency", &out, query, args...)
}

func (r *ClickHouseRepository) GetProcessLatencyByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupLatencyPoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    consumer_group                                                       AS consumer_group,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1])  AS p50,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])  AS p95,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3])  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND (metric_name = @processDuration
		       OR (metric_name = @opDuration AND lower(messaging_operation) IN @processOps))
		  %s
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	var out []GroupLatencyPoint
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetProcessLatencyByGroup", &out, query, args...)
}

func (r *ClickHouseRepository) GetClientOperationDuration(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ClientOpDurationPoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    messaging_operation                                                  AS operation_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1])  AS p50,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])  AS p95,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3])  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @opDuration
		  %s
		GROUP BY time_bucket, operation_name
		ORDER BY time_bucket ASC, operation_name ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	var out []ClientOpDurationPoint
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetClientOperationDuration", &out, query, args...)
}

// ---------------------------------------------------------------------------
// Error-rate helpers — messaging_counters_rollup with error_type key
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) getTopicErrorRates(ctx context.Context, teamID int64, startMs, endMs int64, metricName, caller string, f KafkaFilters) ([]ErrorRatePoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                AS time_bucket,
		    messaging_destination                             AS topic,
		    error_type                                        AS error_type,
		    toFloat64(sum(value_sum)) / @bucketSecs      AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND error_type != ''
		  %s
		GROUP BY time_bucket, topic, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args,
		clickhouse.Named("metricName", metricName),
		clickhouse.Named("bucketSecs", bucketSecs(startMs, endMs)),
	)
	var out []ErrorRatePoint
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.getTopicErrorRates", &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	for i := range out {
		if math.IsNaN(out[i].ErrorRate) {
			out[i].ErrorRate = 0.0
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) getGroupErrorRates(ctx context.Context, teamID int64, startMs, endMs int64, metricName, caller string, f KafkaFilters) ([]ErrorRatePoint, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                AS time_bucket,
		    consumer_group                                    AS consumer_group,
		    error_type                                        AS error_type,
		    toFloat64(sum(value_sum)) / @bucketSecs      AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND error_type != ''
		  %s
		GROUP BY time_bucket, consumer_group, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args,
		clickhouse.Named("metricName", metricName),
		clickhouse.Named("bucketSecs", bucketSecs(startMs, endMs)),
	)
	var out []ErrorRatePoint
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "kafka.getGroupErrorRates", &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	for i := range out {
		if math.IsNaN(out[i].ErrorRate) {
			out[i].ErrorRate = 0.0
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Sample queries — messaging_counters_rollup; node_id dim dropped (aggregated)
// because rollup doesn't key on it. Behavior change documented on PR.
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetConsumerMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]ConsumerMetricSample, error) {
	if len(metricNames) == 0 {
		return []ConsumerMetricSample{}, nil
	}
	table := "observability.spans"
	// Inventory filters are lighter than kafkaFilterClauses — no messaging_system
	// predicate. rollupInventoryFilter mirrors that.
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    consumer_group                                                              AS consumer_group,
		    ''                                                                          AS node_id,
		    metric_name                                                                 AS metric_name,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)      AS value
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		  AND consumer_group != ''
		  %s
		GROUP BY time_bucket, consumer_group, metric_name
		ORDER BY time_bucket ASC, consumer_group ASC, metric_name ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupInventoryFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupInventoryArgs(f)...)
	args = append(args, clickhouse.Named("metricNames", metricNames))
	var out []ConsumerMetricSample
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetConsumerMetricSamples", &out, query, args...)
}

func (r *ClickHouseRepository) GetTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]TopicMetricSample, error) {
	if len(metricNames) == 0 {
		return []TopicMetricSample{}, nil
	}
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    messaging_destination                                                       AS topic,
		    consumer_group                                                              AS consumer_group,
		    metric_name                                                                 AS metric_name,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)      AS value
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		  AND messaging_destination != ''
		  %s
		GROUP BY time_bucket, topic, consumer_group, metric_name
		ORDER BY time_bucket ASC, topic ASC, consumer_group ASC, metric_name ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupInventoryFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupInventoryArgs(f)...)
	args = append(args, clickhouse.Named("metricNames", metricNames))
	var out []TopicMetricSample
	return out, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.GetTopicMetricSamples", &out, query, args...)
}

// stringSet builds a lookup set from a slice for O(1) metric-name classification.
func stringSet(s []string) map[string]struct{} {
	m := make(map[string]struct{}, len(s))
	for _, v := range s {
		m[v] = struct{}{}
	}
	return m
}
