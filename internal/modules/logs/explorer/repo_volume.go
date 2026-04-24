package explorer

import (
	"context"
	"fmt"
	"strings"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

// Summary returns list-header KPIs (total/errors/warns) off the rollup.
// Severity bucket convention: 0=trace..5=fatal; 3=warn, 4+=error.
func (r *Repository) Summary(ctx context.Context, f querycompiler.Filters) (Summary, error) {
	table, _ := rollup.TierTableFor(logsRollupPrefix, f.StartMs, f.EndMs)
	compiled := querycompiler.Compile(f, querycompiler.TargetRollup)
	query := fmt.Sprintf(`
		SELECT severity_bucket AS sb, sumMerge(log_count) AS c
		FROM %s WHERE %s GROUP BY sb`, table, compiled.Where)
	type row struct {
		Sb	uint8	`ch:"sb"`
		C	uint64	`ch:"c"`
	}
	var rows []row
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "explorer.Summary", &rows, query, compiled.Args...); err != nil {
		return Summary{}, err
	}
	var s Summary
	for _, row := range rows {
		s.Total += row.C
		switch {
		case row.Sb >= 4:
			s.Errors += row.C
		case row.Sb == 3:
			s.Warns += row.C
		}
	}
	return s, nil
}

// Trend returns the severity-bucketed volume histogram.
func (r *Repository) Trend(ctx context.Context, f querycompiler.Filters, step string) ([]TrendBucket, []string, error) {
	table, tierStep := rollup.TierTableFor(logsRollupPrefix, f.StartMs, f.EndMs)
	compiled := querycompiler.Compile(f, querycompiler.TargetRollup)
	stepMin := resolveStepMinutes(step, tierStep, f.StartMs, f.EndMs)
	bucketExpr := fmt.Sprintf(
		"formatDateTime(toStartOfInterval(bucket_ts, toIntervalMinute(%d)), '%%Y-%%m-%%d %%H:%%i:00')",
		stepMin,
	)
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       severity_bucket,
		       sumMerge(log_count) AS count
		FROM %s
		WHERE %s
		GROUP BY time_bucket, severity_bucket
		ORDER BY time_bucket ASC`, bucketExpr, table, compiled.Where)
	var rows []trendRowDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "explorer.Trend", &rows, query, compiled.Args...); err != nil {
		return nil, nil, err
	}
	buckets := make([]TrendBucket, len(rows))
	for i, row := range rows {
		buckets[i] = TrendBucket(row)
	}
	return buckets, compiled.DroppedClauses, nil
}

// resolveStepMinutes clamps an explicit step token to the tier's native step.
func resolveStepMinutes(step string, tierStepMin int64, startMs, endMs int64) int64 {
	s := strings.TrimSpace(step)
	if s == "" {
		return adaptiveStepMin(tierStepMin, startMs, endMs)
	}
	explicit, ok := map[string]int64{"1m": 1, "5m": 5, "15m": 15, "1h": 60, "1d": 1440}[s]
	if !ok {
		return adaptiveStepMin(tierStepMin, startMs, endMs)
	}
	if explicit < tierStepMin {
		return tierStepMin
	}
	return explicit
}

func adaptiveStepMin(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var desired int64
	switch {
	case hours <= 3:
		desired = 1
	case hours <= 24:
		desired = 5
	case hours <= 168:
		desired = 60
	default:
		desired = 1440
	}
	if tierStepMin > desired {
		return tierStepMin
	}
	return desired
}
