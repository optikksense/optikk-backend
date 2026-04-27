// Package resource resolves logs resource fingerprints for service/host/pod/
// container/environment filters. Lifted out of explorer/repo_resource.go so
// every logs submodule can apply the same PREWHERE narrowing.
package resource

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

const logsResourceTable = "observability.logs_v2_resource"

// HasFilters reports whether any resource-scoped filters are present.
func HasFilters(f querycompiler.Filters) bool {
	return len(f.Services) > 0 ||
		len(f.Hosts) > 0 ||
		len(f.Pods) > 0 ||
		len(f.Containers) > 0 ||
		len(f.Environments) > 0 ||
		len(f.ExcludeServices) > 0 ||
		len(f.ExcludeHosts) > 0
}

// WithFingerprints narrows preWhere by `resource_fingerprint IN (...)` when
// resource filters are present. Returns empty=true if filters resolve to no
// fingerprints (callers should short-circuit with an empty result).
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

	return preWhere + " AND resource_fingerprint IN @resourceFingerprints",
		append(args, clickhouse.Named("resourceFingerprints", fingerprints)),
		false,
		nil
}

func resolveFingerprints(ctx context.Context, db clickhouse.Conn, f querycompiler.Filters) ([]string, error) {
	query := `
		SELECT DISTINCT resource_fingerprint
		FROM ` + logsResourceTable + `
		PREWHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		WHERE 1 = 1`
	args := []any{
		clickhouse.Named("teamID", uint32(f.TeamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", utils.LogsBucketStart(f.StartMs/1000)),
		clickhouse.Named("bucketEnd", utils.LogsBucketStart(f.EndMs/1000)),
	}

	query, args = appendFilter(query, args, "service", f.Services, false)
	query, args = appendFilter(query, args, "service", f.ExcludeServices, true)
	query, args = appendFilter(query, args, "host", f.Hosts, false)
	query, args = appendFilter(query, args, "host", f.ExcludeHosts, true)
	query, args = appendFilter(query, args, "pod", f.Pods, false)
	query, args = appendFilter(query, args, "container", f.Containers, false)
	query, args = appendFilter(query, args, "environment", f.Environments, false)
	query += ` LIMIT 4096`

	rows, err := dbutil.QueryCH(dbutil.ExplorerCtx(ctx), db, "logs.resolveLogResourceFingerprints", query, args...)
	if err != nil {
		return nil, fmt.Errorf("logs.resolveLogResourceFingerprints: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	var out []string
	for rows.Next() {
		var fingerprint string
		if err := rows.Scan(&fingerprint); err != nil {
			return nil, err
		}
		if fingerprint != "" {
			out = append(out, fingerprint)
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
