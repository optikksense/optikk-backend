package anomaly

import (
	"fmt"
	"math"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// Repository defines the data-access contract for anomaly detection.
type Repository interface {
	GetBaseline(teamID, startMs, endMs int64, req BaselineRequest) ([]BaselinePoint, error)
}

// ClickHouseRepository implements Repository against ClickHouse spans table.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository returns a production ClickHouseRepository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// metricExpr returns the ClickHouse aggregation expression for the requested metric.
func metricExpr(metric string) string {
	switch metric {
	case "error_rate":
		return "countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count()"
	case "latency_p95":
		return "quantile(0.95)(s.duration_nano / 1000000.0)"
	case "request_rate":
		return "count()"
	default:
		return "count()"
	}
}

// GetBaseline queries the current period and a 7-day-ago baseline period,
// then computes per-bucket bounds and anomaly flags.
func (r *ClickHouseRepository) GetBaseline(teamID, startMs, endMs int64, req BaselineRequest) ([]BaselinePoint, error) {
	expr := metricExpr(req.Metric)
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")

	// --- current period ---
	currentRows, err := r.queryPeriod(teamID, startMs, endMs, req.ServiceName, expr, bucket)
	if err != nil {
		return nil, fmt.Errorf("current period query: %w", err)
	}

	// --- baseline period (shifted back 7 days) ---
	const weekMs = 7 * 24 * 60 * 60 * 1000
	baseStartMs := startMs - weekMs
	baseEndMs := endMs - weekMs
	baseBucket := timebucket.ExprForColumn(baseStartMs, baseEndMs, "s.timestamp")

	baselineStats, err := r.queryBaselineStats(teamID, baseStartMs, baseEndMs, req.ServiceName, expr, baseBucket)
	if err != nil {
		return nil, fmt.Errorf("baseline period query: %w", err)
	}

	// --- assemble result ---
	points := make([]BaselinePoint, len(currentRows))
	for i, row := range currentRows {
		val := dbutil.Float64FromAny(row["metric_value"])
		upper := baselineStats.avg + req.Sensitivity*baselineStats.stddev
		lower := baselineStats.avg - req.Sensitivity*baselineStats.stddev
		if lower < 0 {
			lower = 0
		}

		points[i] = BaselinePoint{
			Timestamp:     dbutil.StringFromAny(row["time_bucket"]),
			Value:         val,
			BaselineValue: baselineStats.avg,
			UpperBound:    upper,
			LowerBound:    lower,
			IsAnomaly:     val > upper || val < lower,
		}
	}
	return points, nil
}

type baselineStats struct {
	avg    float64
	stddev float64
}

// queryPeriod returns bucketed metric values for a time range.
func (r *ClickHouseRepository) queryPeriod(teamID, startMs, endMs int64, serviceName, expr, bucket string) ([]map[string]any, error) {
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       %s AS metric_value
		FROM observability.spans s
		WHERE s.team_id = ?
		  AND s.parent_span_id = ''
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?`,
		bucket, expr)

	args := []any{
		teamID,
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs),
		dbutil.SqlTime(endMs),
	}

	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY 1 ORDER BY 1 ASC`

	return dbutil.QueryMaps(r.db, query, args...)
}

// queryBaselineStats computes the overall avg and stddev of bucketed metric
// values over the baseline period.
func (r *ClickHouseRepository) queryBaselineStats(teamID, startMs, endMs int64, serviceName, expr, bucket string) (baselineStats, error) {
	inner := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       %s AS metric_value
		FROM observability.spans s
		WHERE s.team_id = ?
		  AND s.parent_span_id = ''
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?`,
		bucket, expr)

	args := []any{
		teamID,
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs),
		dbutil.SqlTime(endMs),
	}

	if serviceName != "" {
		inner += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	inner += ` GROUP BY 1`

	query := fmt.Sprintf(`SELECT avg(metric_value) AS baseline_avg, stddevPop(metric_value) AS baseline_stddev FROM (%s)`, inner)

	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil {
		return baselineStats{}, err
	}

	avg := dbutil.Float64FromAny(row["baseline_avg"])
	sd := dbutil.Float64FromAny(row["baseline_stddev"])
	if math.IsNaN(avg) {
		avg = 0
	}
	if math.IsNaN(sd) {
		sd = 0
	}
	return baselineStats{avg: avg, stddev: sd}, nil
}
