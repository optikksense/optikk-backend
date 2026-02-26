package store

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/saturation/model"
)

// ClickHouseRepository encapsulates saturation data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new saturation repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSaturationMetrics(teamUUID string, startMs, endMs int64) ([]model.SaturationMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       COUNT(*) as span_count,
		       AVG(duration_ms) as avg_duration_ms,
		       quantile(0.95)(duration_ms) as p95_duration_ms,
		       MAX(duration_ms) as max_duration_ms,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       avg(CAST(JSONExtractFloat(attributes, 'db.connection_pool.utilization') AS Float64)) as avg_db_pool_util,
		       max(CAST(JSONExtractFloat(attributes, 'db.connection_pool.utilization') AS Float64)) as max_db_pool_util,
		       avg(CAST(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') AS Float64)) as avg_consumer_lag,
		       max(CAST(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') AS Float64)) as max_consumer_lag,
		       avg(CAST(JSONExtractFloat(attributes, 'thread.pool.active') AS Float64)) as avg_thread_pool_active,
		       max(CAST(JSONExtractFloat(attributes, 'thread.pool.size') AS Float64)) as max_thread_pool_size,
		       avg(CAST(JSONExtractFloat(attributes, 'queue.depth') AS Float64)) as avg_queue_depth,
		       max(CAST(JSONExtractFloat(attributes, 'queue.depth') AS Float64)) as max_queue_depth
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ? AND service_name != ''
		GROUP BY service_name
		ORDER BY span_count DESC
		LIMIT 50
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	metrics := make([]model.SaturationMetric, len(rows))
	for i, row := range rows {
		metrics[i] = model.SaturationMetric{
			ServiceName:         dbutil.StringFromAny(row["service_name"]),
			SpanCount:           dbutil.Int64FromAny(row["span_count"]),
			AvgDurationMs:       dbutil.Float64FromAny(row["avg_duration_ms"]),
			P95DurationMs:       dbutil.Float64FromAny(row["p95_duration_ms"]),
			MaxDurationMs:       dbutil.Float64FromAny(row["max_duration_ms"]),
			ErrorCount:          dbutil.Int64FromAny(row["error_count"]),
			AvgDbPoolUtil:       dbutil.Float64FromAny(row["avg_db_pool_util"]),
			MaxDbPoolUtil:       dbutil.Float64FromAny(row["max_db_pool_util"]),
			AvgConsumerLag:      dbutil.Float64FromAny(row["avg_consumer_lag"]),
			MaxConsumerLag:      dbutil.Float64FromAny(row["max_consumer_lag"]),
			AvgThreadPoolActive: dbutil.Float64FromAny(row["avg_thread_pool_active"]),
			MaxThreadPoolSize:   dbutil.Float64FromAny(row["max_thread_pool_size"]),
			AvgQueueDepth:       dbutil.Float64FromAny(row["avg_queue_depth"]),
			MaxQueueDepth:       dbutil.Float64FromAny(row["max_queue_depth"]),
		}
	}
	return metrics, nil
}

func (r *ClickHouseRepository) GetSaturationTimeSeries(teamUUID string, startMs, endMs int64) ([]model.SaturationTimeSeries, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       formatDateTime(toStartOfMinute(start_time), '%Y-%m-%dT%H:%i:%SZ') as timestamp,
		       COUNT(*) as span_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_duration_ms,
		       avg(CAST(JSONExtractFloat(attributes, 'db.connection_pool.utilization') AS Float64)) as avg_db_pool_util,
		       avg(CAST(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') AS Float64)) as avg_consumer_lag,
		       avg(CAST(JSONExtractFloat(attributes, 'thread.pool.active') AS Float64)) as avg_thread_active,
		       avg(CAST(JSONExtractFloat(attributes, 'queue.depth') AS Float64)) as avg_queue_depth
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ? AND service_name != ''
		GROUP BY service_name, toStartOfMinute(start_time)
		ORDER BY timestamp ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	timeseries := make([]model.SaturationTimeSeries, len(rows))
	for i, row := range rows {
		timeseries[i] = model.SaturationTimeSeries{
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			Timestamp:       dbutil.StringFromAny(row["timestamp"]),
			SpanCount:       dbutil.Int64FromAny(row["span_count"]),
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			AvgDurationMs:   dbutil.Float64FromAny(row["avg_duration_ms"]),
			AvgDbPoolUtil:   dbutil.Float64FromAny(row["avg_db_pool_util"]),
			AvgConsumerLag:  dbutil.Float64FromAny(row["avg_consumer_lag"]),
			AvgThreadActive: dbutil.Float64FromAny(row["avg_thread_active"]),
			AvgQueueDepth:   dbutil.Float64FromAny(row["avg_queue_depth"]),
		}
	}
	return timeseries, nil
}
