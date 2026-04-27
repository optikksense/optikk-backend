// Package mapper converts OTLP trace export requests into spans/schema.Row wire values.
package mapper

import (
	"encoding/json"
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/infra/fingerprint"
	"github.com/Optikk-Org/optikk-backend/internal/infra/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/enrich"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/schema"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

// MapRequest converts an OTLP trace export request into wire rows; one OTLP span yields one Row.
func MapRequest(teamID int64, req *tracepb.ExportTraceServiceRequest) []*schema.Row {
	rows := make([]*schema.Row, 0, 64)
	for _, rs := range req.GetResourceSpans() {
		var resAttrs []*commonpb.KeyValue
		if rs.Resource != nil {
			resAttrs = rs.Resource.Attributes
		}
		resMap := otlp.AttrsToMap(resAttrs)
		fp := fingerprint.Calculate(resMap)
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				rows = append(rows, buildRow(teamID, resMap, fp, s))
			}
		}
	}
	return rows
}

func buildRow(teamID int64, resMap map[string]string, fingerprint string, s *trace.Span) *schema.Row {
	timestampNs := s.GetStartTimeUnixNano()
	tsBucket := timebucket.SpansBucketStart(int64(timestampNs / nsPerSecond))
	statusMsg, statusCode := spanStatus(s)
	spanMap := otlp.AttrsToMap(s.GetAttributes())
	mergedMap := mergeAndCapAttrs(resMap, spanMap)
	http := extractHTTPFields(spanMap, s.GetKind())
	exc := extractExceptionFields(spanMap)
	stripPromotedKeys(mergedMap)

	return &schema.Row{
		TsBucket:            tsBucket,
		TeamId:              uint32(teamID),     //nolint:gosec // G115 team_id
		TimestampNs:         int64(timestampNs), //nolint:gosec
		TraceId:             enrich.ZeroTraceID(otlp.BytesToHex(s.GetTraceId())),
		SpanId:              enrich.ZeroSpanID(otlp.BytesToHex(s.GetSpanId())),
		ParentSpanId:        enrich.ZeroSpanID(otlp.BytesToHex(s.GetParentSpanId())),
		TraceState:          s.GetTraceState(),
		Flags:               s.GetFlags(),
		Name:                s.GetName(),
		Kind:                int32(s.GetKind()),
		KindString:          spanKindString(s.GetKind()),
		DurationNano:        spanDuration(s),
		HasError:            statusCode == trace.Status_STATUS_CODE_ERROR,
		IsRemote:            false,
		StatusCode:          int32(statusCode),
		StatusCodeString:    statusCodeString(statusCode),
		StatusMessage:       statusMsg,
		HttpUrl:             http.url,
		HttpMethod:          http.method,
		HttpHost:            http.host,
		ExternalHttpUrl:     http.externalURL,
		ExternalHttpMethod:  http.externalMethod,
		ResponseStatusCode:  http.statusCode,
		HttpStatusBucket:    httpStatusBucket(http.statusCode, statusCode == trace.Status_STATUS_CODE_ERROR),
		Service:             resMap["service.name"],
		Host:                resMap["host.name"],
		Pod:                 resMap["k8s.pod.name"],
		ServiceVersion:      resMap["service.version"],
		Environment:         resMap["deployment.environment"],
		PeerService:         spanMap["peer.service"],
		DbSystem:            spanMap["db.system"],
		DbName:              spanMap["db.name"],
		DbStatement:         spanMap["db.statement"],
		HttpRoute:           spanMap["http.route"],
		Attributes:          mergedMap,
		Fingerprint:         fingerprint,
		Events:              serializeEvents(s.GetEvents()),
		Links:               serializeLinks(s.GetLinks()),
		ExceptionType:       exc.typ,
		ExceptionMessage:    exc.message,
		ExceptionStacktrace: exc.stacktrace,
		ExceptionEscaped:    exc.escaped,
	}
}

const nsPerSecond = 1_000_000_000

// spanDuration returns span duration in nanoseconds, or 0 if end <= start.
func spanDuration(s *trace.Span) uint64 {
	if s.EndTimeUnixNano > s.StartTimeUnixNano {
		return s.EndTimeUnixNano - s.StartTimeUnixNano
	}
	return 0
}

// serializeEvents converts span events into one JSON string per event.
func serializeEvents(events []*trace.Span_Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		ev := map[string]any{"name": e.Name}
		if e.TimeUnixNano > 0 {
			ev["timeUnixNano"] = strconv.FormatUint(e.TimeUnixNano, 10)
		}
		if len(e.Attributes) > 0 {
			ev["attributes"] = otlp.AttrsToMap(e.Attributes)
		}
		b, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		out = append(out, string(b))
	}
	return out
}

// serializeLinks converts span links to a single JSON string.
func serializeLinks(links []*trace.Span_Link) string {
	data := make([]map[string]any, 0, len(links))
	for _, lk := range links {
		link := map[string]any{
			"traceId": otlp.BytesToHex(lk.TraceId),
			"spanId":  otlp.BytesToHex(lk.SpanId),
		}
		if len(lk.Attributes) > 0 {
			link["attributes"] = otlp.AttrsToMap(lk.Attributes)
		}
		data = append(data, link)
	}
	b, _ := json.Marshal(data)
	return string(b)
}
