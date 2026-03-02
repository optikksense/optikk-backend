package logs

import (
	"context"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// GetLogHistogram returns time-bucketed log counts by level.
// Reads from the raw logs table with efficient time bucketing.
func (r *ClickHouseRepository) GetLogHistogram(ctx context.Context, f LogFilters, step string) ([]LogHistogramBucket, error) {
	if step == "" {
		step = autoStep(f.StartMs, f.EndMs)
	}

	bucketExpr := logBucketExpr(step)
	where, args := buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, level, COUNT(*) as count
		FROM logs WHERE%s
		GROUP BY %s, level
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	buckets := make([]LogHistogramBucket, 0, len(rows))
	for _, row := range rows {
		buckets = append(buckets, LogHistogramBucket{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			Level:      dbutil.StringFromAny(row["level"]),
			Count:      dbutil.Int64FromAny(row["count"]),
		})
	}
	return buckets, nil
}

// GetLogVolume returns time-bucketed log counts with level breakdown.
// Reads from the raw logs table with efficient time bucketing.
func (r *ClickHouseRepository) GetLogVolume(ctx context.Context, f LogFilters, step string) ([]LogVolumeBucket, error) {
	if step == "" {
		step = autoStep(f.StartMs, f.EndMs)
	}

	bucketExpr := logBucketExpr(step)
	where, args := buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       COUNT(*) as total,
		       sum(if(level='ERROR', 1, 0)) as errors,
		       sum(if(level='WARN', 1, 0)) as warnings,
		       sum(if(level='INFO', 1, 0)) as infos,
		       sum(if(level='DEBUG', 1, 0)) as debugs,
		       sum(if(level='FATAL', 1, 0)) as fatals
		FROM logs WHERE%s
		GROUP BY %s
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	buckets := make([]LogVolumeBucket, 0, len(rows))
	for _, row := range rows {
		buckets = append(buckets, LogVolumeBucket{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			Total:      dbutil.Int64FromAny(row["total"]),
			Errors:     dbutil.Int64FromAny(row["errors"]),
			Warnings:   dbutil.Int64FromAny(row["warnings"]),
			Infos:      dbutil.Int64FromAny(row["infos"]),
			Debugs:     dbutil.Int64FromAny(row["debugs"]),
			Fatals:     dbutil.Int64FromAny(row["fatals"]),
		})
	}

	buckets = fillVolumeBuckets(buckets, f.StartMs, f.EndMs, step)
	return buckets, nil
}
