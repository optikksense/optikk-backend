package api

import (
	"strconv"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/modules/ingestion/model"
)

// TranslateSpans converts OTLP traces payloads into our internal SpanRecord format.
func TranslateSpans(teamUUID string, payload model.OTLPTracesPayload) []model.SpanRecord {
	var spansToInsert []model.SpanRecord

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

				infra := extractInfraLabels(spanAttrs, rc.attrs)

				rootInt := 0
				if span.ParentSpanID == "" {
					rootInt = 1
				}

				spansToInsert = append(spansToInsert, model.SpanRecord{
					TeamUUID:       teamUUID,
					TraceID:        span.TraceID,
					SpanID:         span.SpanID,
					ParentSpanID:   span.ParentSpanID,
					IsRoot:         rootInt,
					OperationName:  span.Name,
					ServiceName:    rc.serviceName,
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

	return spansToInsert
}
