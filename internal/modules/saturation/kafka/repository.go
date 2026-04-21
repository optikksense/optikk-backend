package kafka

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// Kafka panels read from two Phase-7 + Phase-9 rollups:
//
//   - `messaging_histograms_rollup` — publish/receive/process/client-operation
//     duration percentiles (Phase 7). Used by the 4 *Latency* methods.
//   - `messaging_counters_rollup`   — counter + gauge + histogram counts per
//     canonical messaging dim (messaging_destination, consumer_group,
//     messaging_operation, broker, partition, error_type). Phase-9 addition.
//     Used by every rate / lag / rebalance / error / broker / sample method.
//
// `sample_count` in the counters rollup stores `hist_count` for histogram rows
// and 1 for counter rows (see MV in db/clickhouse/23_rollup_messaging_counters.sql),
// so `sumMerge(sample_count) / @bucketSecs` is an operation-rate across both
// types.
//
// Semantic note (carried over from Phase 8): rollup dims come from canonical
// OTel attribute names (`messaging.destination.name`, `messaging.kafka.consumer.group`,
// `messaging.operation`, `server.address`, `messaging.kafka.destination.partition`,
// `error.type`). Non-canonical aliases captured by the legacy `topicExpr()`
// / `consumerGroupExpr()` helpers do NOT flow into the rollup — if ingestion
// emits only aliased attributes for a given tenant, the corresponding panels
// return empty.

const (
	messagingRollupPrefix         = "observability.messaging_histograms_rollup"
	messagingCountersRollupPrefix = "observability.messaging_counters_rollup"
)

// rollupBucketExpr bucketizes the rollup's DateTime `bucket_ts` column.
func rollupBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumnTime(startMs, endMs, "bucket_ts")
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

// ---------------------------------------------------------------------------
// Summary / global panel
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetKafkaSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) (KafkaSummaryStats, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}
	var stats KafkaSummaryStats

	// Rates + max lag from counters rollup.
	countersTable, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	countersQuery := fmt.Sprintf(`
		SELECT
		    metric_name                      AS metric_name,
		    toFloat64(sumMerge(value_sum))   AS v_sum,
		    toFloat64(maxMerge(value_max))   AS v_max
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND (metric_name IN @producerMetrics
		       OR metric_name IN @consumerMetrics
		       OR metric_name IN @lagMetrics)
		  %s
		GROUP BY metric_name
	`, countersTable, rollupTopicGroupFilter(f))
	var rateRows []struct {
		MetricName string  `ch:"metric_name"`
		VSum       float64 `ch:"v_sum"`
		VMax       float64 `ch:"v_max"`
	}
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args,
		clickhouse.Named("producerMetrics", ProducerMetrics),
		clickhouse.Named("consumerMetrics", ConsumerMetrics),
		clickhouse.Named("lagMetrics", ConsumerLagMetrics),
	)
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rateRows, countersQuery, args...); err != nil {
		return stats, err
	}
	prod := stringSet(ProducerMetrics)
	cons := stringSet(ConsumerMetrics)
	lag := stringSet(ConsumerLagMetrics)
	for _, row := range rateRows {
		_, isProd := prod[row.MetricName]
		_, isCons := cons[row.MetricName]
		_, isLag := lag[row.MetricName]
		switch {
		case isProd:
			stats.PublishRatePerSec += row.VSum / durationSecs
		case isCons:
			stats.ReceiveRatePerSec += row.VSum / durationSecs
		case isLag:
			if row.VMax > stats.MaxLag {
				stats.MaxLag = row.VMax
			}
		}
	}

	// Percentiles from histogram rollup. publish_p95 + receive_p95 need the
	// same fan-out as GetPublishLatencyByTopic / GetReceiveLatencyByTopic but
	// aggregated across topics.
	histTable, _ := rollup.TierTableFor(messagingRollupPrefix, startMs, endMs)
	histQuery := fmt.Sprintf(`
		SELECT
		    metric_name                                                           AS metric_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])   AS p95
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN (@publishDuration, @receiveDuration, @opDuration)
		  %s
		GROUP BY metric_name
	`, histTable, rollupTopicGroupFilter(f))
	var histRows []struct {
		MetricName string  `ch:"metric_name"`
		P95        float64 `ch:"p95"`
	}
	histArgs := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &histRows, histQuery, histArgs...); err != nil {
		return stats, err
	}
	for _, row := range histRows {
		switch row.MetricName {
		case MetricPublishDuration:
			stats.PublishP95Ms = row.P95
		case MetricReceiveDuration:
			stats.ReceiveP95Ms = row.P95
		}
	}
	return stats, nil
}

// ---------------------------------------------------------------------------
// Rate panels — messaging_counters_rollup, sumMerge(value_sum) / bucketSecs
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetProduceRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	return r.topicRate(ctx, teamID, startMs, endMs, f, ProducerMetrics)
}

func (r *ClickHouseRepository) GetConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	return r.topicRate(ctx, teamID, startMs, endMs, f, ConsumerMetrics)
}

func (r *ClickHouseRepository) topicRate(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metrics []string) ([]TopicRatePoint, error) {
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                       AS time_bucket,
		    messaging_destination                    AS topic,
		    toFloat64(sumMerge(value_sum)) / @bucketSecs AS rate_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metrics
		  %s
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("bucketSecs", bucketSecs(startMs, endMs)), clickhouse.Named("metrics", metrics))
	var out []TopicRatePoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	return r.groupRate(ctx, teamID, startMs, endMs, f, ConsumerMetrics)
}

func (r *ClickHouseRepository) GetProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	return r.groupRate(ctx, teamID, startMs, endMs, f, ProcessMetrics)
}

func (r *ClickHouseRepository) groupRate(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metrics []string) ([]GroupRatePoint, error) {
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                           AS time_bucket,
		    consumer_group                               AS consumer_group,
		    toFloat64(sumMerge(value_sum)) / @bucketSecs AS rate_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metrics
		  %s
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("bucketSecs", bucketSecs(startMs, endMs)), clickhouse.Named("metrics", metrics))
	var out []GroupRatePoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// Lag — messaging_counters_rollup; avg via value_avg_num / sample_count
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetConsumerLagByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]LagPoint, error) {
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    consumer_group                                                              AS consumer_group,
		    messaging_destination                                                       AS topic,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)      AS lag
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @lagMetrics
		  %s
		GROUP BY time_bucket, consumer_group, topic
		ORDER BY time_bucket ASC, consumer_group ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("lagMetrics", ConsumerLagMetrics))
	var out []LagPoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetConsumerLagPerPartition(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]PartitionLag, error) {
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    messaging_destination                                                       AS topic,
		    toInt64OrZero(partition)                                                    AS partition,
		    consumer_group                                                              AS consumer_group,
		    toInt64(sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)) AS lag
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @lagMetrics
		  %s
		GROUP BY topic, partition, consumer_group
		ORDER BY lag DESC
		LIMIT 200
	`, table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("lagMetrics", ConsumerLagMetrics))
	var out []PartitionLag
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// Rebalance — messaging_counters_rollup; 6 metric_names, fold client-side
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]RebalancePoint, error) {
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    consumer_group                                                              AS consumer_group,
		    metric_name                                                                 AS metric_name,
		    toFloat64(sumMerge(value_sum))                                              AS v_sum,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)      AS v_avg
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
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
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &metricRows, query, args...); err != nil {
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
	table, _ := rollup.TierTableFor(messagingRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    messaging_destination                                                AS topic,
		    metric_name                                                          AS metric_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])  AS p95
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
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
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &metricRows, query, args...); err != nil {
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
// (see MV in db/clickhouse/23_rollup_messaging_counters.sql). Rate is
// errored-ops / bucketSecs.
func (r *ClickHouseRepository) GetClientOpErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                              AS time_bucket,
		    messaging_operation                                             AS operation_name,
		    error_type                                                      AS error_type,
		    toFloat64(sumMerge(sample_count)) / @bucketSecs                 AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @opDuration
		  AND error_type != ''
		  %s
		GROUP BY time_bucket, operation_name, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("bucketSecs", bucketSecs(startMs, endMs)))
	var out []ErrorRatePoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetBrokerConnections(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]BrokerConnectionPoint, error) {
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    broker                                                                      AS broker,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)      AS connections
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, broker
		ORDER BY time_bucket ASC, broker ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	args = append(args, clickhouse.Named("metricName", MetricClientConnections))
	var out []BrokerConnectionPoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
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
	table, _ := rollup.TierTableFor(messagingRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    messaging_destination                                                AS topic,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1])  AS p50,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])  AS p95,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3])  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
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
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetProcessLatencyByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupLatencyPoint, error) {
	table, _ := rollup.TierTableFor(messagingRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    consumer_group                                                       AS consumer_group,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1])  AS p50,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])  AS p95,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3])  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND (metric_name = @processDuration
		       OR (metric_name = @opDuration AND lower(messaging_operation) IN @processOps))
		  %s
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	var out []GroupLatencyPoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetClientOperationDuration(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ClientOpDurationPoint, error) {
	table, _ := rollup.TierTableFor(messagingRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    messaging_operation                                                  AS operation_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1])  AS p50,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2])  AS p95,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3])  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @opDuration
		  %s
		GROUP BY time_bucket, operation_name
		ORDER BY time_bucket ASC, operation_name ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupTopicGroupFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	var out []ClientOpDurationPoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// Error-rate helpers — messaging_counters_rollup with error_type key
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) getTopicErrorRates(ctx context.Context, teamID int64, startMs, endMs int64, metricName, caller string, f KafkaFilters) ([]ErrorRatePoint, error) {
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                AS time_bucket,
		    messaging_destination                             AS topic,
		    error_type                                        AS error_type,
		    toFloat64(sumMerge(value_sum)) / @bucketSecs      AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
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
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	return out, nil
}

func (r *ClickHouseRepository) getGroupErrorRates(ctx context.Context, teamID int64, startMs, endMs int64, metricName, caller string, f KafkaFilters) ([]ErrorRatePoint, error) {
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                AS time_bucket,
		    consumer_group                                    AS consumer_group,
		    error_type                                        AS error_type,
		    toFloat64(sumMerge(value_sum)) / @bucketSecs      AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
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
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
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
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	// Inventory filters are lighter than kafkaFilterClauses — no messaging_system
	// predicate. rollupInventoryFilter mirrors that.
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    consumer_group                                                              AS consumer_group,
		    ''                                                                          AS node_id,
		    metric_name                                                                 AS metric_name,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)      AS value
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		  AND consumer_group != ''
		  %s
		GROUP BY time_bucket, consumer_group, metric_name
		ORDER BY time_bucket ASC, consumer_group ASC, metric_name ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupInventoryFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupInventoryArgs(f)...)
	args = append(args, clickhouse.Named("metricNames", metricNames))
	var out []ConsumerMetricSample
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]TopicMetricSample, error) {
	if len(metricNames) == 0 {
		return []TopicMetricSample{}, nil
	}
	table, _ := rollup.TierTableFor(messagingCountersRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    messaging_destination                                                       AS topic,
		    consumer_group                                                              AS consumer_group,
		    metric_name                                                                 AS metric_name,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)      AS value
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		  AND messaging_destination != ''
		  %s
		GROUP BY time_bucket, topic, consumer_group, metric_name
		ORDER BY time_bucket ASC, topic ASC, consumer_group ASC, metric_name ASC
	`, rollupBucketExpr(startMs, endMs), table, rollupInventoryFilter(f))
	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupInventoryArgs(f)...)
	args = append(args, clickhouse.Named("metricNames", metricNames))
	var out []TopicMetricSample
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// stringSet builds a lookup set from a slice for O(1) metric-name classification.
func stringSet(s []string) map[string]struct{} {
	m := make(map[string]struct{}, len(s))
	for _, v := range s {
		m[v] = struct{}{}
	}
	return m
}
