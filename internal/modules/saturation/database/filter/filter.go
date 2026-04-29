// Package filter owns the typed database-saturation filter shape, OTel
// constants, helper bind producers, and the SQL clause emitter every
// db saturation submodule shares. Mirrors internal/modules/metrics/filter
// + internal/modules/{logs,traces}/filter.
//
// **Source-table assignment** (decided per OTel semconv 1.30+):
//
//   - DB RED panels (volume / errors / latency / collection / system /
//     systems / summary / slowqueries) read `observability.spans`. Each DB
//     call is one span with `duration_nano`, `has_error`,
//     `response_status_code`, the flat `db_system` / `db_name` /
//     `db_statement` columns, and the per-data-point `attributes` JSON
//     (carries `db.operation.name`, `db.collection.name`, `db.namespace`,
//     `error.type`, `server.address`).
//
//   - Connection-pool gauges/histograms (`connections` submodule) read
//     `observability.metrics`. The OTel `db.client.connection.*` family
//     is instrumentation-side: `count` / `max` / `idle.max` /
//     `idle.min` / `pending_requests` / `timeouts` (gauges/counters)
//     and `wait_time` / `create_time` / `use_time` (histograms).
//
// **No phantom rollup table** — `db_histograms_rollup` was never built;
// the previous repo code referenced columns (`latency_ms_digest`,
// `value_sum`, `sample_count`, `db_operation`, `pool_name`,
// `db_connection_state`, etc.) that don't exist on either raw table.
// Latency percentiles emit as a fixed-bucket histogram array Go-side
// via [quantile.FromHistogram].
package filter

import (
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// ---------------------------------------------------------------------------
// OTel semantic-convention names + canonical metric names. Re-exported here
// so consumers don't reach into the internal/shared package.
// ---------------------------------------------------------------------------

const (
	AttrDBSystem         = "db.system"
	AttrDBNamespace      = "db.namespace"
	AttrDBOperationName  = "db.operation.name"
	AttrDBCollectionName = "db.collection.name"
	AttrDBQueryText      = "db.query.text"
	AttrDBResponseStatus = "db.response.status_code"
	AttrErrorType        = "error.type"
	AttrServerAddress    = "server.address"
	AttrServerPort       = "server.port"
	AttrPoolName         = "pool.name"
	AttrConnectionState  = "db.client.connection.state"
)

const (
	MetricDBOperationDuration = "db.client.operation.duration"

	MetricDBConnectionCount      = "db.client.connection.count"
	MetricDBConnectionMax        = "db.client.connection.max"
	MetricDBConnectionIdleMax    = "db.client.connection.idle.max"
	MetricDBConnectionIdleMin    = "db.client.connection.idle.min"
	MetricDBConnectionPendReqs   = "db.client.connection.pending_requests"
	MetricDBConnectionTimeouts   = "db.client.connection.timeouts"
	MetricDBConnectionCreateTime = "db.client.connection.create_time"
	MetricDBConnectionWaitTime   = "db.client.connection.wait_time"
	MetricDBConnectionUseTime    = "db.client.connection.use_time"
)

type Filters struct {
	DBSystem   []string
	Collection []string
	Namespace  []string
	Server     []string
}

var LatencyBucketBoundsMs = []float64{
	1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 1e18,
}

func SpanArgs(teamID, startMs, endMs int64) []any {
	bucketStart, bucketEnd := SpanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func SpanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

func MetricArgs(teamID, startMs, endMs int64, metricName string) []any {
	bucketStart, bucketEnd := MetricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", metricName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func MetricArgsMulti(teamID, startMs, endMs int64, metricNames []string) []any {
	bucketStart, bucketEnd := MetricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricNames", metricNames),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func MetricBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

func BucketWidthSeconds(startMs, endMs int64) float64 {
	hours := float64(endMs-startMs) / 3_600_000.0
	switch {
	case hours <= 3:
		return 60
	case hours <= 24:
		return 300
	case hours <= 168:
		return 3600
	default:
		return 86400
	}
}

func BuildSpanClauses(f Filters) (where string, args []any) {
	if len(f.DBSystem) > 0 {
		where += ` AND db_system IN @dbSystem`
		args = append(args, clickhouse.Named("dbSystem", f.DBSystem))
	}
	if len(f.Collection) > 0 {
		where += ` AND attributes.'` + AttrDBCollectionName + `'::String IN @dbCollection`
		args = append(args, clickhouse.Named("dbCollection", f.Collection))
	}
	if len(f.Namespace) > 0 {
		where += ` AND attributes.'` + AttrDBNamespace + `'::String IN @dbNamespace`
		args = append(args, clickhouse.Named("dbNamespace", f.Namespace))
	}
	if len(f.Server) > 0 {
		where += ` AND attributes.'` + AttrServerAddress + `'::String IN @dbServer`
		args = append(args, clickhouse.Named("dbServer", f.Server))
	}
	return where, args
}

func BuildMetricClauses(f Filters) (where string, args []any) {
	if len(f.DBSystem) > 0 {
		where += ` AND attributes.'` + AttrDBSystem + `'::String IN @dbSystem`
		args = append(args, clickhouse.Named("dbSystem", f.DBSystem))
	}
	if len(f.Server) > 0 {
		where += ` AND attributes.'` + AttrServerAddress + `'::String IN @dbServer`
		args = append(args, clickhouse.Named("dbServer", f.Server))
	}
	return where, args
}

func SpanGroupColumn(attr string) string {
	switch attr {
	case AttrDBSystem:
		return "db_system"
	case AttrDBOperationName:
		return "attributes.'" + AttrDBOperationName + "'::String"
	case AttrDBCollectionName:
		return "attributes.'" + AttrDBCollectionName + "'::String"
	case AttrDBNamespace:
		return "attributes.'" + AttrDBNamespace + "'::String"
	case AttrServerAddress:
		return "attributes.'" + AttrServerAddress + "'::String"
	case AttrErrorType:
		return "attributes.'" + AttrErrorType + "'::String"
	case AttrDBResponseStatus:
		return "attributes.'" + AttrDBResponseStatus + "'::String"
	}
	return ""
}

func LatencyBucketCountsSQL() string {
	var b strings.Builder
	b.WriteString("[")
	prev := "0"
	for i, upperMs := range LatencyBucketBoundsMs {
		if i > 0 {
			b.WriteString(", ")
		}
		// last bucket: catch-all >= prev
		if i == len(LatencyBucketBoundsMs)-1 {
			b.WriteString("countIf(duration_nano >= ")
			b.WriteString(prev)
			b.WriteString(")")
			break
		}
		upperNs := strconv.FormatInt(int64(upperMs*1_000_000), 10)
		b.WriteString("countIf(duration_nano >= ")
		b.WriteString(prev)
		b.WriteString(" AND duration_nano < ")
		b.WriteString(upperNs)
		b.WriteString(")")
		prev = upperNs
	}
	b.WriteString("]")
	return b.String()
}
