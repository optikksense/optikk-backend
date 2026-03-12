package httpmetrics

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetRequestRate(teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error)
	GetRequestDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetActiveRequests(teamID int64, startMs, endMs int64) ([]TimeBucket, error)
	GetRequestBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetResponseBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetClientDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetDNSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetTLSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) queryHistogramSummary(teamID int64, startMs, endMs int64, metricName string) (HistogramSummary, error) {
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
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetRequestRate(teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
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
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetRequestDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricHTTPServerRequestDuration)
}

func (r *ClickHouseRepository) GetActiveRequests(teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
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
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetRequestBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricHTTPServerRequestBodySize)
}

func (r *ClickHouseRepository) GetResponseBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricHTTPServerResponseBodySize)
}

func (r *ClickHouseRepository) GetClientDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricHTTPClientRequestDuration)
}

func (r *ClickHouseRepository) GetDNSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricDNSLookupDuration)
}

func (r *ClickHouseRepository) GetTLSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricTLSConnectDuration)
}
