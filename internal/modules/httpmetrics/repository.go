package httpmetrics

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// Repository defines the data access interface for HTTP metrics.
type Repository interface {
	GetRequestRate(teamUUID string, startMs, endMs int64) ([]StatusCodeBucket, error)
	GetRequestDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetActiveRequests(teamUUID string, startMs, endMs int64) ([]TimeBucket, error)
	GetRequestBodySize(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetResponseBodySize(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetClientDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetDNSDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetTLSDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
}

// ClickHouseRepository implements Repository using ClickHouse.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new ClickHouseRepository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

// queryHistogramSummary is a shared helper for p50/p95/p99/avg histogram queries.
func (r *ClickHouseRepository) queryHistogramSummary(teamUUID string, startMs, endMs int64, metricName string) (HistogramSummary, error) {
	query := fmt.Sprintf(`
		SELECT
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99,
		    avg(hist_sum / nullIf(hist_count, 0))                                     AS avg_val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
	`,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	row, err := dbutil.QueryMap(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return HistogramSummary{}, err
	}
	return HistogramSummary{
		P50: dbutil.Float64FromAny(row["p50"]),
		P95: dbutil.Float64FromAny(row["p95"]),
		P99: dbutil.Float64FromAny(row["p99"]),
		Avg: dbutil.Float64FromAny(row["avg_val"]),
	}, nil
}

func (r *ClickHouseRepository) GetRequestRate(teamUUID string, startMs, endMs int64) ([]StatusCodeBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	statusAttr := attrString(AttrHTTPStatusCode)

	query := fmt.Sprintf(`
		SELECT
		    %s                 AS time_bucket,
		    %s                 AS status_code,
		    toInt64(count())   AS req_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket, status_code
		ORDER BY time_bucket, status_code
	`,
		bucket, statusAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricHTTPServerRequestDuration,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]StatusCodeBucket, len(rows))
	for i, row := range rows {
		results[i] = StatusCodeBucket{
			Timestamp:  dbutil.StringFromAny(row["time_bucket"]),
			StatusCode: dbutil.StringFromAny(row["status_code"]),
			Count:      dbutil.Int64FromAny(row["req_count"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetRequestDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamUUID, startMs, endMs, MetricHTTPServerRequestDuration)
}

func (r *ClickHouseRepository) GetActiveRequests(teamUUID string, startMs, endMs int64) ([]TimeBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    avg(value) AS val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricHTTPServerActiveRequests,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]TimeBucket, len(rows))
	for i, row := range rows {
		v := dbutil.NullableFloat64FromAny(row["val"])
		results[i] = TimeBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Value:     v,
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetRequestBodySize(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamUUID, startMs, endMs, MetricHTTPServerRequestBodySize)
}

func (r *ClickHouseRepository) GetResponseBodySize(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamUUID, startMs, endMs, MetricHTTPServerResponseBodySize)
}

func (r *ClickHouseRepository) GetClientDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamUUID, startMs, endMs, MetricHTTPClientRequestDuration)
}

func (r *ClickHouseRepository) GetDNSDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamUUID, startMs, endMs, MetricDNSLookupDuration)
}

func (r *ClickHouseRepository) GetTLSDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamUUID, startMs, endMs, MetricTLSConnectDuration)
}
