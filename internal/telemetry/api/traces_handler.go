package api

import (
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/telemetry/model"
)

// HandleTraces is the Gin handler for POST /otlp/v1/traces.
func (h *Handler) HandleTraces(c *gin.Context) {
	teamUUID, payload, ok := decodeRequest(h, c, protoToTracesPayload)
	if !ok {
		return
	}

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
				httpStatusCode := 0
				if codeStr := firstNonEmpty(spanAttrs["http.response.status_code"], spanAttrs["http.status_code"]); codeStr != "" {
					httpStatusCode, _ = strconv.Atoi(codeStr)
				}

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

	if len(spansToInsert) > 0 {
		if err := h.Ingester.IngestSpans(c.Request.Context(), spansToInsert); err != nil {
			log.Printf("otlp: failed to ingest %d spans: %v", len(spansToInsert), err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ingest spans"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"accepted": len(spansToInsert)})
}
