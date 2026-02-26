package translate

import (
	"strconv"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/modules/ingestion/model"
)

// SpansTranslator converts OTLP traces payloads into SpanRecord slices.
type SpansTranslator struct{}

func (SpansTranslator) Translate(teamUUID string, payload model.OTLPTracesPayload) []model.SpanRecord {
	var spans []model.SpanRecord

	for _, rs := range payload.ResourceSpans {
		rc := newResourceContext(rs.Resource.Attributes)

		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				spanAttrs := otlpAttrMap(span.Attributes)
				allAttrs := mergeOTLPAttrs(rc.attrs, spanAttrs)

				startTime := nanosToTime(span.StartTimeUnixNano)
				endTime := nanosToTime(span.EndTimeUnixNano)
				durationMs := endTime.Sub(startTime).Milliseconds()
				if durationMs < 0 {
					durationMs = 0
				}

				status := "OK"
				statusMessage := ""
				if span.Status != nil {
					if span.Status.Code == 2 {
						status = "ERROR"
					}
					statusMessage = span.Status.Message
				}

				httpMethod := firstNonEmpty(spanAttrs["http.request.method"], spanAttrs["http.method"])
				httpURL := firstNonEmpty(spanAttrs["url.full"], spanAttrs["http.url"], spanAttrs["http.route"])
				httpStatusCode, _ := strconv.Atoi(firstNonEmpty(spanAttrs["http.response.status_code"], spanAttrs["http.status_code"]))
				if status != "ERROR" && httpStatusCode >= 400 {
					status = "ERROR"
					if statusMessage == "" {
						statusMessage = "HTTP " + strconv.Itoa(httpStatusCode)
					}
				}

				operationName := firstNonEmpty(
					span.Name,
					spanAttrs["http.route"],
					spanAttrs["rpc.method"],
					spanAttrs["db.operation"],
					"unknown",
				)
				serviceName := firstNonEmpty(spanAttrs["service.name"], rc.serviceName, "unknown")

				infra := extractInfraLabels(spanAttrs, rc.attrs)

				rootInt := 0
				if span.ParentSpanID == "" {
					rootInt = 1
				}

				spans = append(spans, model.SpanRecord{
					TeamUUID:       teamUUID,
					TraceID:        span.TraceID,
					SpanID:         span.SpanID,
					ParentSpanID:   span.ParentSpanID,
					IsRoot:         rootInt,
					OperationName:  operationName,
					ServiceName:    serviceName,
					SpanKind:       spanKindString(span.Kind),
					StartTime:      startTime,
					EndTime:        endTime,
					DurationMs:     durationMs,
					Status:         status,
					StatusMessage:  statusMessage,
					HTTPMethod:     httpMethod,
					HTTPURL:        httpURL,
					HTTPStatusCode: httpStatusCode,
					Host:           infra.host,
					Pod:            infra.pod,
					Container:      infra.container,
					Attributes:     dbutil.JSONString(allAttrs),
				})
			}
		}
	}

	return spans
}

func spanKindString(kind int) string {
	switch kind {
	case 1:
		return "INTERNAL"
	case 2:
		return "SERVER"
	case 3:
		return "CLIENT"
	case 4:
		return "PRODUCER"
	case 5:
		return "CONSUMER"
	default:
		return "INTERNAL"
	}
}
