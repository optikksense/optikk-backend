package analytics

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// TracesScopeConfig returns the ScopeConfig for traces/spans analytics.
func TracesScopeConfig() ScopeConfig {
	return ScopeConfig{
		Table:           "observability.spans",
		TableAlias:      "s",
		TimestampColumn: "s.timestamp",
		TimeBucketFunc:  tracesTimeBucket,
		BaseWhereFunc:   tracesBaseWhere,
		Dimensions:      tracesDimensions,
		AggFields:       tracesAggFields,
	}
}

var tracesDimensions = map[string]string{
	"service.name":          "s.service_name",
	"service":               "s.service_name",
	"operation":             "s.name",
	"operation_name":        "s.name",
	"span.kind":             "s.kind_string",
	"status":                "s.status_code_string",
	"http.route":            "s.mat_http_route",
	"http.status_code":      "s.mat_http_status_code",
	"http.method":           "s.http_method",
	"http.target":           "s.mat_http_target",
	"http.scheme":           "s.mat_http_scheme",
	"db.system":             "s.mat_db_system",
	"db.name":               "s.mat_db_name",
	"db.operation":          "s.mat_db_operation",
	"rpc.system":            "s.mat_rpc_system",
	"rpc.service":           "s.mat_rpc_service",
	"rpc.method":            "s.mat_rpc_method",
	"rpc.grpc.status_code":  "s.mat_rpc_grpc_status_code",
	"messaging.system":      "s.mat_messaging_system",
	"messaging.operation":   "s.mat_messaging_operation",
	"messaging.destination": "s.mat_messaging_destination",
	"peer.service":          "s.mat_peer_service",
	"net.peer.name":         "s.mat_net_peer_name",
	"exception.type":        "s.mat_exception_type",
	"host.name":             "s.mat_host_name",
	"k8s.pod.name":          "s.mat_k8s_pod_name",
	"response_status_code":  "s.response_status_code",
}

var tracesAggFields = map[string]string{
	"duration_nano": "s.duration_nano",
	"duration_ms":   "s.duration_nano / 1000000.0",
}

func tracesTimeBucket(startMs, endMs int64, step string) string {
	if step != "" {
		strat := utils.ByName(step)
		return strat.GetBucketExpression()
	}
	return utils.Expression(startMs, endMs)
}

func tracesBaseWhere(_ int64, startMs, endMs int64) (string, []any) {
	frag := "s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end"
	args := []any{
		clickhouse.Named("teamID", uint32(0)), // placeholder — caller sets real teamID
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
	return frag, args
}
