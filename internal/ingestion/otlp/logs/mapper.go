package logs

import (
	"hash/fnv"
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/protoconv"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"go.uber.org/zap"
)

const maxLogAttributes = 128
const nsPerSecond = 1_000_000_000

var logColumns = []string{
	"team_id", "ts_bucket_start", "timestamp", "observed_timestamp",
	"id", "trace_id", "span_id", "trace_flags",
	"severity_text", "severity_number", "body",
	"attributes_string", "attributes_number", "attributes_bool",
	"resource", "resource_fingerprint",
	"scope_name", "scope_version", "scope_string",
}

type scopeInfo struct {
	name, version string
	attrs         map[string]string
}

// mapLogs converts an OTLP logs export request into ClickHouse ingest rows.
func mapLogs(teamID int64, req *logspb.ExportLogsServiceRequest) []ingest.Row {
	var rows []ingest.Row
	for _, rl := range req.ResourceLogs {
		var resAttrs []*commonpb.KeyValue
		if rl.Resource != nil {
			resAttrs = rl.Resource.Attributes
		}
		resourceMap := protoconv.AttrsToMap(resAttrs)
		fingerprint := strconv.FormatUint(protoconv.ResourceFingerprint(resAttrs), 16)
		for _, sl := range rl.ScopeLogs {
			scope := extractScope(sl.Scope)
			for _, lr := range sl.LogRecords {
				rows = append(rows, buildLogRow(teamID, resourceMap, fingerprint, scope, lr))
			}
		}
	}
	return rows
}

// buildLogRow maps a single OTLP log record into a ClickHouse ingest row.
func buildLogRow(teamID int64, resourceMap map[string]string, fingerprint string, scope scopeInfo, lr *logv1.LogRecord) ingest.Row {
	tsNs, observedNs := resolveTimestamps(lr)
	tsBucket := timebucket.LogsBucketStart(int64(tsNs / nsPerSecond)) //nolint:gosec // G115
	body := protoconv.AnyValueString(lr.Body)
	attrStr, attrNum, attrBool := protoAttrsToTypedMaps(lr.Attributes)
	attrStr = capStringAttrs(attrStr, teamID)

	return ingest.Row{Values: []any{
		uint32(teamID), //nolint:gosec // G115 — team_id
		tsBucket,       // ts_bucket_start
		tsNs,           // timestamp
		observedNs,     // observed_timestamp
		protoLogID(teamID, tsNs, lr.TraceId, lr.SpanId, body), // id
		protoconv.BytesToHex(lr.TraceId),                      // trace_id
		protoconv.BytesToHex(lr.SpanId),                       // span_id
		lr.Flags,                                              // trace_flags
		resolveSeverity(lr),                                   // severity_text
		uint8(lr.SeverityNumber),                              //nolint:gosec // G115 — severity_number
		body,                                                  // body
		attrStr,                                               // attributes_string
		attrNum,                                               // attributes_number
		attrBool,                                              // attributes_bool
		resourceMap,                                           // resource
		fingerprint,                                           // resource_fingerprint
		scope.name,                                            // scope_name
		scope.version,                                         // scope_version
		scope.attrs,                                           // scope_string
	}}
}

// resolveTimestamps picks the best timestamp, falling back to now().
func resolveTimestamps(lr *logv1.LogRecord) (tsNs, observedNs uint64) {
	tsNs = lr.TimeUnixNano
	if tsNs == 0 {
		tsNs = lr.ObservedTimeUnixNano
	}
	if tsNs == 0 {
		tsNs = uint64(time.Now().UnixNano()) //nolint:gosec // G115
	}
	observedNs = lr.ObservedTimeUnixNano
	if observedNs == 0 {
		observedNs = uint64(time.Now().UnixNano()) //nolint:gosec // G115
	}
	return tsNs, observedNs
}

// resolveSeverity returns severity text, falling back to the numeric level name.
func resolveSeverity(lr *logv1.LogRecord) string {
	if lr.SeverityText != "" {
		return lr.SeverityText
	}
	return severityNumberToLevel(lr.SeverityNumber)
}

// extractScope extracts scope metadata from an instrumentation scope.
func extractScope(scope *commonpb.InstrumentationScope) scopeInfo {
	if scope == nil {
		return scopeInfo{attrs: map[string]string{}}
	}
	attrs := map[string]string{}
	if scope.Name != "" {
		attrs["name"] = scope.Name
	}
	return scopeInfo{name: scope.Name, version: scope.Version, attrs: attrs}
}

// capStringAttrs truncates string attributes to maxLogAttributes.
func capStringAttrs(attrs map[string]string, teamID int64) map[string]string {
	if len(attrs) <= maxLogAttributes {
		return attrs
	}
	logger.L().Warn("ingest: log attributes truncated",
		zap.Int("from", len(attrs)), zap.Int("to", maxLogAttributes),
		zap.Int64("team_id", teamID))
	trimmed := make(map[string]string, maxLogAttributes)
	i := 0
	for k, v := range attrs {
		trimmed[k] = v
		i++
		if i == maxLogAttributes {
			break
		}
	}
	return trimmed
}

func protoAttrsToTypedMaps(kvs []*commonpb.KeyValue) (strMap map[string]string, numMap map[string]float64, boolMap map[string]bool) {
	sm := make(map[string]string, len(kvs))
	nm := make(map[string]float64)
	bm := make(map[string]bool)
	for _, kv := range kvs {
		if kv.Value == nil {
			continue
		}
		switch val := kv.Value.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			sm[kv.Key] = val.StringValue
		case *commonpb.AnyValue_IntValue:
			nm[kv.Key] = float64(val.IntValue)
		case *commonpb.AnyValue_DoubleValue:
			nm[kv.Key] = val.DoubleValue
		case *commonpb.AnyValue_BoolValue:
			bm[kv.Key] = val.BoolValue
		case *commonpb.AnyValue_BytesValue:
			sm[kv.Key] = string(val.BytesValue)
		}
	}
	return sm, nm, bm
}

func protoLogID(teamID int64, tsNano uint64, traceID, spanID []byte, body string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strconv.FormatInt(teamID, 10)))
	_, _ = h.Write([]byte{0})
	b := strconv.AppendUint(nil, tsNano, 10)
	_, _ = h.Write(b)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(traceID)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(spanID)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(body))
	return strconv.FormatUint(h.Sum64(), 16)
}

func severityNumberToLevel(n logv1.SeverityNumber) string {
	v := int(n)
	switch {
	case v <= 0:
		return "UNSET"
	case v <= 4:
		return "TRACE"
	case v <= 8:
		return "DEBUG"
	case v <= 12:
		return "INFO"
	case v <= 16:
		return "WARN"
	case v <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}
