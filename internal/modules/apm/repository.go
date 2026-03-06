package apm

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// Repository defines the data access interface for APM metrics.
type Repository interface {
	GetRPCDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetRPCRequestRate(teamUUID string, startMs, endMs int64) ([]TimeBucket, error)
	GetMessagingPublishDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetProcessCPU(teamUUID string, startMs, endMs int64) ([]StateBucket, error)
	GetProcessMemory(teamUUID string, startMs, endMs int64) (ProcessMemory, error)
	GetOpenFDs(teamUUID string, startMs, endMs int64) ([]TimeBucket, error)
	GetUptime(teamUUID string, startMs, endMs int64) ([]TimeBucket, error)
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

// queryTimeBuckets is a shared helper for simple scalar timeseries.
func (r *ClickHouseRepository) queryTimeBuckets(teamUUID string, startMs, endMs int64, metricName string) ([]TimeBucket, error) {
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
		ColMetricName, metricName,
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

func (r *ClickHouseRepository) GetRPCDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamUUID, startMs, endMs, MetricRPCServerDuration)
}

func (r *ClickHouseRepository) GetRPCRequestRate(teamUUID string, startMs, endMs int64) ([]TimeBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s              AS time_bucket,
		    toInt64(count()) AS val
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
		ColMetricName, MetricRPCServerDuration,
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

func (r *ClickHouseRepository) GetMessagingPublishDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamUUID, startMs, endMs, MetricMessagingPublishDuration)
}

func (r *ClickHouseRepository) GetProcessCPU(teamUUID string, startMs, endMs int64) ([]StateBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	stateAttr := attrString(AttrProcessCPUState)

	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    %s         AS state,
		    avg(value) AS val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket, state
		ORDER BY time_bucket, state
	`,
		bucket, stateAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricProcessCPUTime,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]StateBucket, len(rows))
	for i, row := range rows {
		v := dbutil.NullableFloat64FromAny(row["val"])
		results[i] = StateBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			State:     dbutil.StringFromAny(row["state"]),
			Value:     v,
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetProcessMemory(teamUUID string, startMs, endMs int64) (ProcessMemory, error) {
	query := fmt.Sprintf(`
		SELECT
		    avgIf(value, %s = '%s') AS rss,
		    avgIf(value, %s = '%s') AS vms
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')
	`,
		ColMetricName, MetricProcessMemoryUsage,
		ColMetricName, MetricProcessMemoryVirtual,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricProcessMemoryUsage, MetricProcessMemoryVirtual,
	)
	row, err := dbutil.QueryMap(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return ProcessMemory{}, err
	}
	return ProcessMemory{
		RSS: dbutil.Float64FromAny(row["rss"]),
		VMS: dbutil.Float64FromAny(row["vms"]),
	}, nil
}

func (r *ClickHouseRepository) GetOpenFDs(teamUUID string, startMs, endMs int64) ([]TimeBucket, error) {
	return r.queryTimeBuckets(teamUUID, startMs, endMs, MetricProcessOpenFDs)
}

func (r *ClickHouseRepository) GetUptime(teamUUID string, startMs, endMs int64) ([]TimeBucket, error) {
	return r.queryTimeBuckets(teamUUID, startMs, endMs, MetricProcessUptime)
}
