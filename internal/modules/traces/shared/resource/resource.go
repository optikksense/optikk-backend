// Package resource resolves spans resource fingerprints for service / environment filters via observability.spans_resource.
package resource

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

const tracesResourceTable = "observability.spans_resource"

func HasFilters(f querycompiler.Filters) bool {
	return len(f.Services) > 0 ||
		len(f.ExcludeServices) > 0 ||
		len(f.Environments) > 0
}

// WithFingerprints narrows preWhere by `fingerprint IN (...)` when filters are present; empty=true means filters resolved to nothing.
func WithFingerprints(
	ctx context.Context,
	db clickhouse.Conn,
	f querycompiler.Filters,
	preWhere string,
	args []any,
) (string, []any, bool, error) {
	if !HasFilters(f) {
		return preWhere, args, false, nil
	}

	fingerprints, err := resolveFingerprints(ctx, db, f)
	if err != nil {
		return "", nil, false, err
	}
	if len(fingerprints) == 0 {
		return "", nil, true, nil
	}

	return preWhere + " AND fingerprint IN @resourceFingerprints",
		append(args, clickhouse.Named("resourceFingerprints", fingerprints)),
		false,
		nil
}

func resolveFingerprints(ctx context.Context, db clickhouse.Conn, f querycompiler.Filters) ([]string, error) {
	query := `
		SELECT DISTINCT fingerprint
		FROM ` + tracesResourceTable + `
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE 1 = 1`
	args := []any{
		clickhouse.Named("teamID", uint32(f.TeamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(f.StartMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(f.EndMs/1000)),
	}

	query, args = appendFilter(query, args, "service", f.Services, false)
	query, args = appendFilter(query, args, "service", f.ExcludeServices, true)
	query, args = appendFilter(query, args, "environment", f.Environments, false)
	query += ` LIMIT 4096`

	rows, err := dbutil.QueryCH(dbutil.ExplorerCtx(ctx), db, "traces.resolveSpanResourceFingerprints", query, args...)
	if err != nil {
		return nil, fmt.Errorf("traces.resolveSpanResourceFingerprints: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var out []string
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err != nil {
			return nil, err
		}
		if fp != "" {
			out = append(out, fp)
		}
	}
	return out, rows.Err()
}

func appendFilter(query string, args []any, col string, vals []string, negated bool) (string, []any) {
	if len(vals) == 0 {
		return query, args
	}
	name := fmt.Sprintf("%s_%d", col, len(args))
	if negated {
		query += fmt.Sprintf(" AND %s NOT IN @%s", col, name)
	} else {
		query += fmt.Sprintf(" AND %s IN @%s", col, name)
	}
	return query, append(args, clickhouse.Named(name, vals))
}
