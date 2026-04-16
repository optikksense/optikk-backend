package spans

import (
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/protoconv"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxSpanAttributes = 128

// SpanColumns is the ClickHouse insert column order for observability.spans.
var SpanColumns = []string{
	"ts_bucket_start", "team_id",
	"timestamp", "trace_id", "span_id", "parent_span_id", "trace_state", "flags",
	"name", "kind", "kind_string", "duration_nano", "has_error", "is_remote",
	"status_code", "status_code_string", "status_message",
	"http_url", "http_method", "http_host", "external_http_url", "external_http_method",
	"response_status_code",
	"attributes", "events", "links",
	"exception_type", "exception_message", "exception_stacktrace", "exception_escaped",
}

// SpanRow structurally maps an OTLP span to ClickHouse columns.
type SpanRow struct {
	TsBucketStart       uint64            `ch:"ts_bucket_start"`
	TeamID              uint32            `ch:"team_id"`
	Timestamp           time.Time         `ch:"timestamp"`
	TraceID             string            `ch:"trace_id"`
	SpanID              string            `ch:"span_id"`
	ParentSpanID        string            `ch:"parent_span_id"`
	TraceState          string            `ch:"trace_state"`
	Flags               uint32            `ch:"flags"`
	Name                string            `ch:"name"`
	Kind                int8              `ch:"kind"`
	KindString          string            `ch:"kind_string"`
	DurationNano        uint64            `ch:"duration_nano"`
	HasError            bool              `ch:"has_error"`
	IsRemote            bool              `ch:"is_remote"`
	StatusCode          int16             `ch:"status_code"`
	StatusCodeString    string            `ch:"status_code_string"`
	StatusMessage       string            `ch:"status_message"`
	HTTPUrl             string            `ch:"http_url"`
	HTTPMethod          string            `ch:"http_method"`
	HTTPHost            string            `ch:"http_host"`
	ExternalHTTPUrl     string            `ch:"external_http_url"`
	ExternalHTTPMethod  string            `ch:"external_http_method"`
	ResponseStatusCode  string            `ch:"response_status_code"`
	Attributes          map[string]string `ch:"attributes"`
	Events              []string          `ch:"events"`
	Links               string            `ch:"links"`
	ExceptionType       string            `ch:"exception_type"`
	ExceptionMessage    string            `ch:"exception_message"`
	ExceptionStacktrace string            `ch:"exception_stacktrace"`
	ExceptionEscaped    bool              `ch:"exception_escaped"`
}

type httpFields struct {
	url, method, host, statusCode string
	externalURL, externalMethod   string
}

type exceptionFields struct {
	typ, message, stacktrace string
	escaped                  bool
}

// mapSpans converts an OTLP traces export request into internal Protobuf messages for Kafka transport.
func mapSpans(teamID int64, req *tracepb.ExportTraceServiceRequest) []*proto.SpanRow {
	result := make([]*proto.SpanRow, 0, 64)
	for _, rs := range req.ResourceSpans {
		var resAttrs []*commonpb.KeyValue
		if rs.Resource != nil {
			resAttrs = rs.Resource.Attributes
		}
		resMap := protoconv.AttrsToMap(resAttrs)
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				result = append(result, buildSpanRow(teamID, resMap, s).ToProto())
			}
		}
	}
	return result
}

// buildSpanRow maps a single OTLP span into a ClickHouse ingest row.
func buildSpanRow(teamID int64, resMap map[string]string, s *trace.Span) *SpanRow {
	timestamp := protoconv.NanoToTime(s.StartTimeUnixNano)
	durNano := spanDuration(s)
	tsBucket := utils.SpansBucketStart(timestamp.Unix())
	statusMsg, statusCode := spanStatus(s)
	spanMap := protoconv.AttrsToMap(s.Attributes)
	mergedMap := mergeAndCapAttrs(teamID, s.SpanId, resMap, spanMap)
	http := extractHTTPFields(spanMap, s.Kind)
	exc := extractExceptionFields(spanMap)

	return &SpanRow{
		TsBucketStart:       tsBucket,
		TeamID:              uint32(teamID), //nolint:gosec // G115 — team_id
		Timestamp:           timestamp,
		TraceID:             protoconv.BytesToHex(s.TraceId),
		SpanID:              protoconv.BytesToHex(s.SpanId),
		ParentSpanID:        protoconv.BytesToHex(s.ParentSpanId),
		TraceState:          s.TraceState,
		Flags:               s.Flags,
		Name:                s.Name,
		Kind:                int8(s.Kind), //nolint:gosec // G115 — kind
		KindString:          spanKindString(s.Kind),
		DurationNano:        durNano,
		HasError:            statusCode == trace.Status_STATUS_CODE_ERROR,
		IsRemote:            false,
		StatusCode:          int16(statusCode), //nolint:gosec // G115 — status_code
		StatusCodeString:    statusCodeString(statusCode),
		StatusMessage:       statusMsg,
		HTTPUrl:             http.url,
		HTTPMethod:          http.method,
		HTTPHost:            http.host,
		ExternalHTTPUrl:     http.externalURL,
		ExternalHTTPMethod:  http.externalMethod,
		ResponseStatusCode:  http.statusCode,
		Attributes:          mergedMap,
		Events:              serializeEvents(s.Events),
		Links:               serializeLinks(s.Links),
		ExceptionType:       exc.typ,
		ExceptionMessage:    exc.message,
		ExceptionStacktrace: exc.stacktrace,
		ExceptionEscaped:    exc.escaped,
	}
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
	slog.Warn("ingest: span attributes truncated",
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

func SpanLiveTailStreamPayload(row *SpanRow, emitMs int64) (any, error) {
	w := liveSpanTailFromRow(row)
	w.EmitMs = emitMs
	return w, nil
}

func liveSpanTailFromRow(row *SpanRow) livetail.WireSpan {
	return livetail.WireSpan{
		SpanID:        row.SpanID,
		TraceID:       row.TraceID,
		ServiceName:   row.Attributes["service.name"],
		Host:          row.Attributes["host.name"],
		OperationName: row.Name,
		DurationMs:    float64(row.DurationNano) / 1e6,
		Status:        row.StatusCodeString,
		HTTPMethod:    row.HTTPMethod,
		HTTPStatus:    row.ResponseStatusCode,
		SpanKind:      row.KindString,
		HasError:      row.HasError,
		Timestamp:     row.Timestamp,
		EmitMs:        0, // placeholder, will be set by caller
	}
}

func (row *SpanRow) ToProto() *proto.SpanRow {
	return &proto.SpanRow{
		TsBucketStart:       row.TsBucketStart,
		TeamID:              row.TeamID,
		Timestamp:           timestamppb.New(row.Timestamp),
		TraceID:             row.TraceID,
		SpanID:              row.SpanID,
		ParentSpanID:        row.ParentSpanID,
		TraceState:          row.TraceState,
		Flags:               row.Flags,
		Name:                row.Name,
		Kind:                int32(row.Kind),
		KindString:          row.KindString,
		DurationNano:        row.DurationNano,
		HasError:            row.HasError,
		IsRemote:            row.IsRemote,
		StatusCode:          int32(row.StatusCode),
		StatusCodeString:    row.StatusCodeString,
		StatusMessage:       row.StatusMessage,
		HTTPUrl:             row.HTTPUrl,
		HTTPMethod:          row.HTTPMethod,
		HTTPHost:            row.HTTPHost,
		ExternalHTTPUrl:     row.ExternalHTTPUrl,
		ExternalHTTPMethod:  row.ExternalHTTPMethod,
		ResponseStatusCode:  row.ResponseStatusCode,
		Attributes:          row.Attributes,
		Events:              row.Events,
		Links:               row.Links,
		ExceptionType:       row.ExceptionType,
		ExceptionMessage:    row.ExceptionMessage,
		ExceptionStacktrace: row.ExceptionStacktrace,
		ExceptionEscaped:    row.ExceptionEscaped,
	}
}

func FromProto(p *proto.SpanRow) *SpanRow {
	return &SpanRow{
		TsBucketStart:       p.TsBucketStart,
		TeamID:              p.TeamID,
		Timestamp:           p.Timestamp.AsTime(),
		TraceID:             p.TraceID,
		SpanID:              p.SpanID,
		ParentSpanID:        p.ParentSpanID,
		TraceState:          p.TraceState,
		Flags:               p.Flags,
		Name:                p.Name,
		Kind:                int8(p.Kind),
		KindString:          p.KindString,
		DurationNano:        p.DurationNano,
		HasError:            p.HasError,
		IsRemote:            p.IsRemote,
		StatusCode:          int16(p.StatusCode),
		StatusCodeString:    p.StatusCodeString,
		StatusMessage:       p.StatusMessage,
		HTTPUrl:             p.HTTPUrl,
		HTTPMethod:          p.HTTPMethod,
		HTTPHost:            p.HTTPHost,
		ExternalHTTPUrl:     p.ExternalHTTPUrl,
		ExternalHTTPMethod:  p.ExternalHTTPMethod,
		ResponseStatusCode:  p.ResponseStatusCode,
		Attributes:          p.Attributes,
		Events:              p.Events,
		Links:               p.Links,
		ExceptionType:       p.ExceptionType,
		ExceptionMessage:    p.ExceptionMessage,
		ExceptionStacktrace: p.ExceptionStacktrace,
		ExceptionEscaped:    p.ExceptionEscaped,
	}
}
