package logs

import (
	"strconv"
	"time"

	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/protoconv"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

const maxLogAttributes = 128
const nsPerSecond = 1_000_000_000

// LogColumns is the ClickHouse insert column order for observability.logs.
var LogColumns = []string{
	"team_id", "ts_bucket_start", "timestamp", "observed_timestamp",
	"trace_id", "span_id", "trace_flags", "severity_text", "severity_number", "body",
	"attributes_string", "attributes_number", "attributes_bool",
	"resource", "resource_fingerprint",
	"scope_name", "scope_version", "scope_string",
}

// LogRow structurally maps an OTLP log to ClickHouse columns.
type LogRow struct {
	TeamID              uint32             `ch:"team_id"`
	TsBucketStart       uint32             `ch:"ts_bucket_start"`
	Timestamp           time.Time          `ch:"timestamp"`
	ObservedTimestamp   uint64             `ch:"observed_timestamp"`
	TraceID             string             `ch:"trace_id"`
	SpanID              string             `ch:"span_id"`
	TraceFlags          uint32             `ch:"trace_flags"`
	SeverityText        string             `ch:"severity_text"`
	SeverityNumber      uint8              `ch:"severity_number"`
	Body                string             `ch:"body"`
	AttributesString    map[string]string  `ch:"attributes_string"`
	AttributesNumber    map[string]float64 `ch:"attributes_number"`
	AttributesBool      map[string]bool    `ch:"attributes_bool"`
	Resource            map[string]string  `ch:"resource"`
	ResourceFingerprint string             `ch:"resource_fingerprint"`
	ScopeName           string             `ch:"scope_name"`
	ScopeVersion        string             `ch:"scope_version"`
	ScopeString         map[string]string  `ch:"scope_string"`
}

type scopeInfo struct {
	name, version string
	attrs         map[string]string
}

// mapLogs converts an OTLP logs export request into ClickHouse ingest rows.
func mapLogs(teamID int64, req *logspb.ExportLogsServiceRequest) []*LogRow {
	var rows []*LogRow
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
func buildLogRow(teamID int64, resourceMap map[string]string, fingerprint string, scope scopeInfo, lr *logv1.LogRecord) *LogRow {
	tsNs, observedNs := resolveTimestamps(lr)
	tsBucket := utils.LogsBucketStart(int64(tsNs / nsPerSecond))
	body := protoconv.AnyValueString(lr.Body)
	attrStr, attrNum, attrBool := protoAttrsToTypedMaps(lr.Attributes)
	attrStr = capStringAttrs(attrStr, teamID)

	return &LogRow{
		TeamID:              uint32(teamID), //nolint:gosec // G115 — team_id
		TsBucketStart:       tsBucket,
		Timestamp:           protoconv.NanoToTime(tsNs),
		ObservedTimestamp:   observedNs,
		TraceID:             protoconv.BytesToHex(lr.TraceId),
		SpanID:              protoconv.BytesToHex(lr.SpanId),
		TraceFlags:          lr.Flags,
		SeverityText:        resolveSeverity(lr),
		SeverityNumber:      uint8(lr.SeverityNumber), //nolint:gosec // G115 — severity_number
		Body:                body,
		AttributesString:    attrStr,
		AttributesNumber:    attrNum,
		AttributesBool:      attrBool,
		Resource:            resourceMap,
		ResourceFingerprint: fingerprint,
		ScopeName:           scope.name,
		ScopeVersion:        scope.version,
		ScopeString:         scope.attrs,
	}
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
	slog.Warn("ingest: log attributes truncated",
		slog.Int("from", len(attrs)), slog.Int("to", maxLogAttributes),
		slog.Int64("team_id", teamID))
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

// WireLog mirrors the logs API JSON shape (internal/modules/logs/internal/shared.Log).
type WireLog struct {
	Timestamp         uint64             `json:"timestamp"`
	ObservedTimestamp uint64             `json:"observed_timestamp"`
	SeverityText      string             `json:"severity_text"`
	SeverityNumber    uint8              `json:"severity_number"`
	Body              string             `json:"body"`
	TraceID           string             `json:"trace_id"`
	SpanID            string             `json:"span_id"`
	TraceFlags        uint32             `json:"trace_flags"`
	ServiceName       string             `json:"service_name"`
	Host              string             `json:"host"`
	Pod               string             `json:"pod"`
	Container         string             `json:"container"`
	Environment       string             `json:"environment"`
	AttributesString  map[string]string  `json:"attributes_string,omitempty"`
	AttributesNumber  map[string]float64 `json:"attributes_number,omitempty"`
	AttributesBool    map[string]bool    `json:"attributes_bool,omitempty"`
	ScopeName         string             `json:"scope_name"`
	ScopeVersion      string             `json:"scope_version"`
	Level             string             `json:"level"`
	Message           string             `json:"message"`
	Service           string             `json:"service"`
	EmitMs            int64              `json:"emit_ms"`
}

func wireLogFromRow(row *LogRow) WireLog {
	return WireLog{
		Timestamp:         uint64(row.Timestamp.UnixNano()),
		ObservedTimestamp: row.ObservedTimestamp,
		SeverityText:      row.SeverityText,
		SeverityNumber:    row.SeverityNumber,
		Body:              row.Body,
		TraceID:           row.TraceID,
		SpanID:            row.SpanID,
		TraceFlags:        row.TraceFlags,
		ServiceName:       row.Resource["service.name"],
		Host:              row.Resource["host.name"],
		Pod:               row.Resource["k8s.pod.name"],
		Container:         row.Resource["k8s.container.name"],
		Environment:       row.Resource["deployment.environment"],
		AttributesString:  row.AttributesString,
		AttributesNumber:  row.AttributesNumber,
		AttributesBool:    row.AttributesBool,
		ScopeName:         row.ScopeName,
		ScopeVersion:      row.ScopeVersion,
		Level:             row.SeverityText,
		Message:           row.Body,
		Service:           row.Resource["service.name"],
	}
}

// LiveTailStreamPayload builds a WireLog object for livetail:log:streaming.
func LiveTailStreamPayload(row *LogRow) (any, bool) {
	w := wireLogFromRow(row)
	w.EmitMs = time.Now().UnixMilli()
	return w, true
}
