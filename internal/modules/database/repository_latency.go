package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// GetLatencyBySystem — p50/p95/p99 per time bucket grouped by db.system.
func (r *ClickHouseRepository) GetLatencyBySystem(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(teamID, startMs, endMs, AttrDBSystem, f)
}

// GetLatencyByOperation — grouped by db.operation.name.
func (r *ClickHouseRepository) GetLatencyByOperation(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(teamID, startMs, endMs, AttrDBOperationName, f)
}

// GetLatencyByCollection — grouped by db.collection.name.
func (r *ClickHouseRepository) GetLatencyByCollection(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(teamID, startMs, endMs, AttrDBCollectionName, f)
}

// GetLatencyByNamespace — grouped by db.namespace.
func (r *ClickHouseRepository) GetLatencyByNamespace(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(teamID, startMs, endMs, AttrDBNamespace, f)
}

// GetLatencyByServer — grouped by server.address.
func (r *ClickHouseRepository) GetLatencyByServer(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return r.latencySeriesByAttr(teamID, startMs, endMs, AttrServerAddress, f)
}

// GetLatencyHeatmap — time × latency bucket density for all queries.
// OTel histogram buckets: 0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 10, +Inf (seconds).
func (r *ClickHouseRepository) GetLatencyHeatmap(teamID int64, startMs, endMs int64, f Filters) ([]LatencyHeatmapBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)

	// We approximate by classifying each hist_sum/hist_count point into ms buckets.
	query := fmt.Sprintf(`
		SELECT
		    %s                                                                    AS time_bucket,
		    multiIf(
		        hist_sum / nullIf(hist_count, 0) < 0.001,  '< 1ms',
		        hist_sum / nullIf(hist_count, 0) < 0.005,  '1–5ms',
		        hist_sum / nullIf(hist_count, 0) < 0.010,  '5–10ms',
		        hist_sum / nullIf(hist_count, 0) < 0.025,  '10–25ms',
		        hist_sum / nullIf(hist_count, 0) < 0.050,  '25–50ms',
		        hist_sum / nullIf(hist_count, 0) < 0.100,  '50–100ms',
		        hist_sum / nullIf(hist_count, 0) < 0.250,  '100–250ms',
		        hist_sum / nullIf(hist_count, 0) < 0.500,  '250–500ms',
		        hist_sum / nullIf(hist_count, 0) < 1.000,  '500ms–1s',
		        '> 1s'
		    )                                                                     AS bucket_label,
		    toInt64(sum(hist_count))                                              AS count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND hist_count > 0
		  %s
		GROUP BY time_bucket, bucket_label
		ORDER BY time_bucket, bucket_label
	`,
		bucket,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		fc,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, fargs)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Compute density per time bucket.
	type key struct{ tb, label string }
	counts := map[string]int64{}
	items := make([]struct {
		tb, label string
		count     int64
	}, len(rows))

	for i, row := range rows {
		tb := dbutil.StringFromAny(row["time_bucket"])
		lbl := dbutil.StringFromAny(row["bucket_label"])
		cnt := dbutil.Int64FromAny(row["count"])
		items[i] = struct {
			tb, label string
			count     int64
		}{tb, lbl, cnt}
		counts[tb] += cnt
	}

	out := make([]LatencyHeatmapBucket, len(items))
	for i, it := range items {
		total := counts[it.tb]
		density := 0.0
		if total > 0 {
			density = float64(it.count) / float64(total)
		}
		out[i] = LatencyHeatmapBucket{
			TimeBucket:  it.tb,
			BucketLabel: it.label,
			Count:       it.count,
			Density:     density,
		}
	}
	return out, nil
}
