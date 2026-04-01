package spans

import (
	"encoding/json"
	"log/slog"
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/protoconv"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

const maxSpanAttributes = 128

var spanColumns = []string{
	"ts_bucket_start", "team_id",
	"timestamp", "trace_id", "span_id", "parent_span_id", "trace_state", "flags",
	"name", "kind", "kind_string", "duration_nano", "has_error", "is_remote",
	"status_code", "status_code_string", "status_message",
	"http_url", "http_method", "http_host", "external_http_url", "external_http_method",
	"response_status_code",
	"attributes", "events", "links",
	"exception_type", "exception_message", "exception_stacktrace", "exception_escaped",
}

type httpFields struct {
	url, method, host, statusCode string
	externalURL, externalMethod   string
}

type exceptionFields struct {
	typ, message, stacktrace string
	escaped                  bool
}

// mapSpans converts an OTLP trace export request into ClickHouse ingest rows.
func mapSpans(teamID int64, req *tracepb.ExportTraceServiceRequest) []ingest.Row {
	result := make([]ingest.Row, 0, 64)
	for _, rs := range req.ResourceSpans {
		var resAttrs []*commonpb.KeyValue
		if rs.Resource != nil {
			resAttrs = rs.Resource.Attributes
		}
		resMap := protoconv.AttrsToMap(resAttrs)
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				result = append(result, buildSpanRow(teamID, resMap, s))
			}
		}
	}
	return result
}

// buildSpanRow maps a single OTLP span into a ClickHouse ingest row.
func buildSpanRow(teamID int64, resMap map[string]string, s *trace.Span) ingest.Row {
	timestamp := protoconv.NanoToTime(s.StartTimeUnixNano)
	durNano := spanDuration(s)
	tsBucket := timebucket.SpansBucketStart(timestamp.Unix())
	statusMsg, statusCode := spanStatus(s)
	spanMap := protoconv.AttrsToMap(s.Attributes)
	mergedMap := mergeAndCapAttrs(teamID, s.SpanId, resMap, spanMap)
	http := extractHTTPFields(spanMap, s.Kind)
	exc := extractExceptionFields(spanMap)

	return ingest.Row{Values: []any{
		tsBucket,                             // ts_bucket_start
		uint32(teamID),                       //nolint:gosec // G115 — team_id
		timestamp,                            // timestamp
		protoconv.BytesToHex(s.TraceId),      // trace_id
		protoconv.BytesToHex(s.SpanId),       // span_id
		protoconv.BytesToHex(s.ParentSpanId), // parent_span_id
		s.TraceState,                         // trace_state
		s.Flags,                              // flags
		s.Name,                               // name
		int8(s.Kind),                         //nolint:gosec // G115 — kind
		spanKindString(s.Kind),               // kind_string
		durNano,                              // duration_nano
		statusCode == trace.Status_STATUS_CODE_ERROR, // has_error
		false,                        // is_remote
		int16(statusCode),            //nolint:gosec // G115 — status_code
		statusCodeString(statusCode), // status_code_string
		statusMsg,                    // status_message
		http.url,                     // http_url
		http.method,                  // http_method
		http.host,                    // http_host
		http.externalURL,             // external_http_url
		http.externalMethod,          // external_http_method
		http.statusCode,              // response_status_code
		mergedMap,                    // attributes
		serializeEvents(s.Events),    // events
		serializeLinks(s.Links),      // links
		exc.typ,                      // exception_type
		exc.message,                  // exception_message
		exc.stacktrace,               // exception_stacktrace
		exc.escaped,                  // exception_escaped
	}}
}

// spanDuration returns span duration in nanoseconds, or 0 if end <= start.
func spanDuration(s *trace.Span) uint64 {
	if s.EndTimeUnixNano > s.StartTimeUnixNano {
		return s.EndTimeUnixNano - s.StartTimeUnixNano
	}
	return 0
}

// spanStatus extracts message and code, defaulting to UNSET.
func spanStatus(s *trace.Span) (string, trace.Status_StatusCode) {
	if s.Status != nil {
		return s.Status.Message, s.Status.Code
	}
	return "", trace.Status_STATUS_CODE_UNSET
}

// mergeAndCapAttrs merges resource+span attrs (span wins), capping at maxSpanAttributes.
func mergeAndCapAttrs(teamID int64, spanID []byte, resMap, spanMap map[string]string) map[string]string {
	merged := make(map[string]string, len(resMap)+len(spanMap))
	for k, v := range resMap {
		merged[k] = v
	}
	for k, v := range spanMap {
		merged[k] = v
	}
	if len(merged) <= maxSpanAttributes {
		return merged
	}
	logger.L().Warn("ingest: span attributes truncated",
		slog.Int("from", len(merged)), slog.Int("to", maxSpanAttributes),
		slog.Int64("team_id", teamID), slog.String("span_id", protoconv.BytesToHex(spanID)))
	trimmed := make(map[string]string, maxSpanAttributes)
	i := 0
	for k, v := range merged {
		trimmed[k] = v
		i++
		if i == maxSpanAttributes {
			break
		}
	}
	return trimmed
}

// extractHTTPFields pulls HTTP semantic convention attrs; external fields set for CLIENT spans.
func extractHTTPFields(spanMap map[string]string, kind trace.Span_SpanKind) httpFields {
	h := httpFields{
		method:     mapGet(spanMap, "http.method", "http.request.method"),
		url:        mapGet(spanMap, "http.url", "url.full"),
		host:       mapGet(spanMap, "http.host", "net.host.name"),
		statusCode: mapGet(spanMap, "http.status_code", "http.response.status_code"),
	}
	if kind == trace.Span_SPAN_KIND_CLIENT {
		h.externalURL = h.url
		h.externalMethod = h.method
	}
	return h
}

// extractExceptionFields pulls exception semantic convention attrs.
func extractExceptionFields(spanMap map[string]string) exceptionFields {
	return exceptionFields{
		typ:        spanMap["exception.type"],
		message:    spanMap["exception.message"],
		stacktrace: spanMap["exception.stacktrace"],
		escaped:    spanMap["exception.escaped"] == "true",
	}
}

// serializeEvents converts span events to a JSON string array.
func serializeEvents(events []*trace.Span_Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		ev := map[string]any{"name": e.Name}
		if e.TimeUnixNano > 0 {
			ev["timeUnixNano"] = strconv.FormatUint(e.TimeUnixNano, 10)
		}
		if len(e.Attributes) > 0 {
			ev["attributes"] = protoconv.AttrsToMap(e.Attributes)
		}
		b, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		out = append(out, string(b))
	}
	return out
}

// serializeLinks converts span links to a JSON string.
func serializeLinks(links []*trace.Span_Link) string {
	data := make([]map[string]any, 0, len(links))
	for _, lk := range links {
		link := map[string]any{
			"traceId": protoconv.BytesToHex(lk.TraceId),
			"spanId":  protoconv.BytesToHex(lk.SpanId),
		}
		if len(lk.Attributes) > 0 {
			link["attributes"] = protoconv.AttrsToMap(lk.Attributes)
		}
		data = append(data, link)
	}
	b, _ := json.Marshal(data)
	return string(b)
}

func spanKindString(kind trace.Span_SpanKind) string {
	switch kind {
	case trace.Span_SPAN_KIND_INTERNAL:
		return "INTERNAL"
	case trace.Span_SPAN_KIND_SERVER:
		return "SERVER"
	case trace.Span_SPAN_KIND_CLIENT:
		return "CLIENT"
	case trace.Span_SPAN_KIND_PRODUCER:
		return "PRODUCER"
	case trace.Span_SPAN_KIND_CONSUMER:
		return "CONSUMER"
	default:
		return "UNSPECIFIED"
	}
}

func statusCodeString(code trace.Status_StatusCode) string {
	switch code {
	case trace.Status_STATUS_CODE_OK:
		return "OK"
	case trace.Status_STATUS_CODE_ERROR:
		return "ERROR"
	default:
		return "UNSET"
	}
}

func mapGet(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}
