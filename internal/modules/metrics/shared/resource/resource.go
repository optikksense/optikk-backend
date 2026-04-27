// Package resource resolves metrics resource fingerprints for service / host / environment / k8s_namespace filters via the metrics_resource dictionary.
package resource

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

const Table = "observability.metrics_resource"

// BucketBounds returns the hour-aligned [start, end) bucket range covering [startMs, endMs].
func BucketBounds(startMs, endMs int64) (bucketStart, bucketEnd time.Time) {
	bucketStart = timebucket.MetricsHourBucket(startMs / 1000)
	bucketEnd = timebucket.MetricsHourBucket(endMs / 1000).Add(time.Hour)
	return
}

var keyAliases = map[string]string{
	"service":                "service",
	"service.name":           "service",
	"host":                   "host",
	"host.name":              "host",
	"environment":            "environment",
	"deployment.environment": "environment",
	"k8s_namespace":          "k8s_namespace",
	"k8s.namespace.name":     "k8s_namespace",
}

func Canonical(key string) string {
	return keyAliases[key]
}

func IsResourceKey(key string) bool {
	_, ok := keyAliases[key]
	return ok
}

// ColumnExpr returns the CH expression to extract a resource value from raw observability.metrics.
func ColumnExpr(canonicalKey string) string {
	switch canonicalKey {
	case "service":
		return "resource.`service.name`::String"
	case "host":
		return "resource.`host.name`::String"
	case "environment":
		return "resource.`deployment.environment`::String"
	case "k8s_namespace":
		return "resource.`k8s.namespace.name`::String"
	}
	return ""
}

type Filters struct {
	TeamID  int64
	StartMs int64
	EndMs   int64

	Services      []string
	Hosts         []string
	Environments  []string
	K8sNamespaces []string

	ExcludeServices      []string
	ExcludeHosts         []string
	ExcludeEnvironments  []string
	ExcludeK8sNamespaces []string
}

func HasFilters(f Filters) bool {
	return len(f.Services) > 0 ||
		len(f.Hosts) > 0 ||
		len(f.Environments) > 0 ||
		len(f.K8sNamespaces) > 0 ||
		len(f.ExcludeServices) > 0 ||
		len(f.ExcludeHosts) > 0 ||
		len(f.ExcludeEnvironments) > 0 ||
		len(f.ExcludeK8sNamespaces) > 0
}

// WithFingerprints narrows where by `fingerprint IN (...)` when filters are present; empty=true means filters resolved to nothing.
func WithFingerprints(
	ctx context.Context,
	db clickhouse.Conn,
	f Filters,
	where string,
	args []any,
) (string, []any, bool, error) {
	if !HasFilters(f) {
		return where, args, false, nil
	}

	fingerprints, err := resolveFingerprints(ctx, db, f)
	if err != nil {
		return "", nil, false, err
	}
	if len(fingerprints) == 0 {
		return "", nil, true, nil
	}

	return where + " AND fingerprint IN @resourceFingerprints",
		append(args, clickhouse.Named("resourceFingerprints", fingerprints)),
		false,
		nil
}

func resolveFingerprints(ctx context.Context, db clickhouse.Conn, f Filters) ([]string, error) {
	bucketStart, bucketEnd := BucketBounds(f.StartMs, f.EndMs)

	query := `
		SELECT DISTINCT fingerprint
		FROM ` + Table + `
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE 1 = 1`
	args := []any{
		clickhouse.Named("teamID", uint32(f.TeamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
	}

	query, args = appendFilter(query, args, "service", f.Services, false)
	query, args = appendFilter(query, args, "service", f.ExcludeServices, true)
	query, args = appendFilter(query, args, "host", f.Hosts, false)
	query, args = appendFilter(query, args, "host", f.ExcludeHosts, true)
	query, args = appendFilter(query, args, "environment", f.Environments, false)
	query, args = appendFilter(query, args, "environment", f.ExcludeEnvironments, true)
	query, args = appendFilter(query, args, "k8s_namespace", f.K8sNamespaces, false)
	query, args = appendFilter(query, args, "k8s_namespace", f.ExcludeK8sNamespaces, true)
	query += ` LIMIT 4096`

	rows, err := dbutil.QueryCH(dbutil.ExplorerCtx(ctx), db, "metrics.resolveResourceFingerprints", query, args...)
	if err != nil {
		return nil, fmt.Errorf("metrics.resolveResourceFingerprints: %w", err)
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
