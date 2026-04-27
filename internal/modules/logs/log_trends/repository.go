package log_trends //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/resource"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/step"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// Summary returns list-header KPIs from narrowed raw rows. Severity bucket
// convention: 0=trace..5=fatal; 3=warn, 4+=error.
func (r *Repository) Summary(ctx context.Context, f querycompiler.Filters) (models.Summary, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetRaw)
	preWhere, args, empty, err := resource.WithFingerprints(ctx, r.db, f, compiled.PreWhere, compiled.Args)
	if err != nil {
		return models.Summary{}, err
	}
	if empty {
		return models.Summary{}, nil
	}

	rows, err := dbutil.QueryCH(dbutil.OverviewCtx(ctx), r.db, "logsTrends.Summary", fmt.Sprintf(`
		SELECT severity_bucket
		FROM %s
		PREWHERE %s
		WHERE %s
	`, models.RawLogsTable, preWhere, compiled.Where), args...)
	if err != nil {
		return models.Summary{}, err
	}
	defer rows.Close() //nolint:errcheck

	var out models.Summary
	for rows.Next() {
		var bucket uint8
		if err := rows.Scan(&bucket); err != nil {
			return models.Summary{}, err
		}
		out.Total++
		switch {
		case bucket >= 4:
			out.Errors++
		case bucket == 3:
			out.Warns++
		}
	}
	return out, rows.Err()
}

// Trend returns the severity-bucketed volume histogram, reduced in Go after a
// PREWHERE-pruned raw-row scan.
func (r *Repository) Trend(ctx context.Context, f querycompiler.Filters, stepToken string) ([]models.TrendBucket, []string, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetRaw)
	preWhere, args, empty, err := resource.WithFingerprints(ctx, r.db, f, compiled.PreWhere, compiled.Args)
	if err != nil {
		return nil, nil, err
	}
	if empty {
		return nil, nil, nil
	}

	stepMin := step.ResolveStepMinutes(stepToken, 1, f.StartMs, f.EndMs)
	rows, err := dbutil.QueryCH(dbutil.OverviewCtx(ctx), r.db, "logsTrends.Trend", fmt.Sprintf(`
		SELECT timestamp, severity_bucket
		FROM %s
		PREWHERE %s
		WHERE %s
		ORDER BY timestamp ASC
	`, models.RawLogsTable, preWhere, compiled.Where), args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close() //nolint:errcheck

	type key struct {
		bucket   string
		severity uint8
	}
	counts := make(map[key]uint64)
	for rows.Next() {
		var ts time.Time
		var severity uint8
		if err := rows.Scan(&ts, &severity); err != nil {
			return nil, nil, err
		}
		bucket := step.FormatBucket(ts, stepMin)
		counts[key{bucket: bucket, severity: severity}]++
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	keys := make([]key, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].bucket == keys[j].bucket {
			return keys[i].severity < keys[j].severity
		}
		return keys[i].bucket < keys[j].bucket
	})

	out := make([]models.TrendBucket, 0, len(keys))
	for _, k := range keys {
		out = append(out, models.TrendBucket{
			TimeBucket: k.bucket,
			Severity:   k.severity,
			Count:      counts[k],
		})
	}
	return out, compiled.DroppedClauses, nil
}
