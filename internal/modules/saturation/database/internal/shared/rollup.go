package shared

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	)

// QueryIntervalMinutes returns the group-by step (in minutes) for rollup reads, never finer than the tier's native resolution.
func QueryIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var dashStep int64
	switch {
	case hours <= 3:
		dashStep = 1
	case hours <= 24:
		dashStep = 5
	case hours <= 168:
		dashStep = 60
	default:
		dashStep = 1440
	}
	return dashStep
}

// RollupFilterClauses translates Filters into db_histograms_rollup column predicates.
func RollupFilterClauses(f Filters) (frag string, args []any) {
	appendIn := func(col, prefix string, values []string) {
		if len(values) == 0 {
			return
		}
		frag += " AND " + col + " IN @" + prefix
		args = append(args, clickhouse.Named(prefix, values))
	}
	appendIn("db_system", "dbSystem", f.DBSystem)
	appendIn("db_collection", "dbCollection", f.Collection)
	appendIn("db_namespace", "dbNamespace", f.Namespace)
	appendIn("server_address", "dbServer", f.Server)
	return frag, args
}

// RollupBaseParams returns the standard (teamID, start, end, metricName) named args for rollup reads.
func RollupBaseParams(teamID int64, startMs, endMs int64, metricName string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // domain-bounded team id
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", metricName),
	}
}

// GroupColumnFor maps a shared.AttrX constant to a db_histograms_rollup key column, or "" when none applies.
func GroupColumnFor(attr string) string {
	switch attr {
	case AttrDBSystem:
		return "db_system"
	case AttrDBOperationName:
		return "db_operation"
	case AttrDBCollectionName:
		return "db_collection"
	case AttrDBNamespace:
		return "db_namespace"
	case AttrServerAddress:
		return "server_address"
	case AttrErrorType:
		return "error_type"
	case AttrPoolName:
		return "pool_name"
	case AttrConnectionState:
		return "db_connection_state"
	case AttrDBResponseStatus:
		return "db_response_status_code"
	}
	return ""
}

// BucketTimeExpr renders the stored ts_bucket as a YYYY-MM-DD HH:MM:SS string for the time_bucket column.
const BucketTimeExpr = "toString(ts_bucket)"
