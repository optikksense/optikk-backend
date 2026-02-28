package impl

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/insights/model"
)

// ClickHouseRepository encapsulates data access logic for insights.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new Insights Repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}


// GetInsightSloSli queries SLO compliance status and timeseries.
func (r *ClickHouseRepository) GetInsightSloSli(teamUUID string, startMs, endMs int64, serviceName string) (model.SloSummary, []model.SloBucket, error) {
	query1 := `
		SELECT sum(count) as total_requests,
		       sum(if(status='ERROR', count, 0)) as error_count,
		       if(sum(count)>0, (sum(count)-sum(if(status='ERROR', count, 0)))*100.0/sum(count), 100.0) as availability_percent,
		       avg(avg) as avg_latency_ms,
		       avg(p95) as p95_latency_ms
		FROM metrics
		WHERE team_id = ? AND metric_category = 'http' AND timestamp BETWEEN ? AND ?
	`
	args1 := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query1 += ` AND service_name = ?`
		args1 = append(args1, serviceName)
	}

	summaryRaw, err := dbutil.QueryMap(r.db, query1, args1...)
	if err != nil {
		return model.SloSummary{}, nil, err
	}

	summary := model.SloSummary{
		TotalRequests:       dbutil.Int64FromAny(summaryRaw["total_requests"]),
		ErrorCount:          dbutil.Int64FromAny(summaryRaw["error_count"]),
		AvailabilityPercent: dbutil.Float64FromAny(summaryRaw["availability_percent"]),
		AvgLatencyMs:        dbutil.Float64FromAny(summaryRaw["avg_latency_ms"]),
		P95LatencyMs:        dbutil.Float64FromAny(summaryRaw["p95_latency_ms"]),
	}

	query2 := `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%d %H:%i:00') as time_bucket,
		       sum(count) as request_count,
		       sum(if(status='ERROR', count, 0)) as error_count,
		       if(sum(count)>0, (sum(count)-sum(if(status='ERROR', count, 0)))*100.0/sum(count), 100.0) as availability_percent,
		       avg(avg) as avg_latency_ms
		FROM metrics
		WHERE team_id = ? AND metric_category = 'http' AND timestamp BETWEEN ? AND ?
	`
	args2 := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query2 += ` AND service_name = ?`
		args2 = append(args2, serviceName)
	}
	query2 += ` GROUP BY 1 ORDER BY 1 ASC`

	timeseriesRaw, err := dbutil.QueryMaps(r.db, query2, args2...)
	if err != nil {
		return model.SloSummary{}, nil, err
	}

	timeseries := make([]model.SloBucket, len(timeseriesRaw))
	for i, row := range timeseriesRaw {
		timeseries[i] = model.SloBucket{
			Timestamp:           dbutil.StringFromAny(row["time_bucket"]),
			RequestCount:        dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:          dbutil.Int64FromAny(row["error_count"]),
			AvailabilityPercent: dbutil.Float64FromAny(row["availability_percent"]),
			AvgLatencyMs:        dbutil.NullableFloat64FromAny(row["avg_latency_ms"]),
		}
	}

	return summary, timeseries, nil
}

// GetInsightLogsStream queries log stream, volume trend, and facets.
func (r *ClickHouseRepository) GetInsightLogsStream(teamUUID string, startMs, endMs int64, limit int) ([]model.LogStreamItem, int64, []model.LogVolumeBucket, []model.Facet, []model.Facet, error) {
	streamRaw, err := dbutil.QueryMaps(r.db, `
		SELECT timestamp, level, service_name, logger, message, trace_id, span_id,
		       host, pod, container, thread, exception
		FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC
		LIMIT ?
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), limit)
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	stream := make([]model.LogStreamItem, len(streamRaw))
	for i, row := range streamRaw {
		stream[i] = model.LogStreamItem{
			Timestamp:   dbutil.StringFromAny(row["timestamp"]),
			Level:       dbutil.StringFromAny(row["level"]),
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			Logger:      dbutil.StringFromAny(row["logger"]),
			Message:     dbutil.StringFromAny(row["message"]),
			TraceID:     dbutil.StringFromAny(row["trace_id"]),
			SpanID:      dbutil.StringFromAny(row["span_id"]),
			Host:        dbutil.StringFromAny(row["host"]),
			Pod:         dbutil.StringFromAny(row["pod"]),
			Container:   dbutil.StringFromAny(row["container"]),
			Thread:      dbutil.StringFromAny(row["thread"]),
			Exception:   dbutil.StringFromAny(row["exception"]),
		}
	}

	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE team_id = ? AND timestamp BETWEEN ? AND ?`,
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	volumeRaw, err := dbutil.QueryMaps(r.db, `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%d %H:%i:00') as time_bucket,
		       COUNT(*) as log_count,
		       sum(if(trace_id != '', 1, 0)) as correlated_log_count
		FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY 1
		ORDER BY 1 ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	volume := make([]model.LogVolumeBucket, len(volumeRaw))
	for i, row := range volumeRaw {
		volume[i] = model.LogVolumeBucket{
			Timestamp:          dbutil.StringFromAny(row["time_bucket"]),
			LogCount:           dbutil.Int64FromAny(row["log_count"]),
			CorrelatedLogCount: dbutil.Int64FromAny(row["correlated_log_count"]),
		}
	}

	levelFacetsRaw, err := dbutil.QueryMaps(r.db, `
		SELECT level as name, COUNT(*) as count FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY level ORDER BY count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	levelFacets := make([]model.Facet, len(levelFacetsRaw))
	for i, row := range levelFacetsRaw {
		levelFacets[i] = model.Facet{
			Name:  dbutil.StringFromAny(row["name"]),
			Count: dbutil.Int64FromAny(row["count"]),
		}
	}

	serviceFacetsRaw, err := dbutil.QueryMaps(r.db, `
		SELECT service_name as name, COUNT(*) as count FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY service_name ORDER BY count DESC LIMIT 20
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	serviceFacets := make([]model.Facet, len(serviceFacetsRaw))
	for i, row := range serviceFacetsRaw {
		serviceFacets[i] = model.Facet{
			Name:  dbutil.StringFromAny(row["name"]),
			Count: dbutil.Int64FromAny(row["count"]),
		}
	}

	return stream, total, volume, levelFacets, serviceFacets, nil
}

