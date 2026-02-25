package telemetry

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// HandleTraces is the Gin handler for POST /otlp/v1/traces.
func (h *Handler) HandleTraces(c *gin.Context) {
	teamUUID, ok := h.resolveAPIKey(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing api_key"})
		return
	}

	body, err := readBody(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	var payload OTLPTracesPayload
	if isProtobuf(c) {
		payload, err = protoToTracesPayload(body)
	} else {
		err = json.Unmarshal(body, &payload)
	}
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid OTLP payload"})
		return
	}

	var spansToInsert []SpanRecord

	for _, rs := range payload.ResourceSpans {
		resourceAttrs := otlpAttrMap(rs.Resource.Attributes)
		serviceName := resourceAttrs["service.name"]
		if serviceName == "" {
			serviceName = "unknown"
		}

		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				spanAttrs := otlpAttrMap(span.Attributes)
				allAttrs := mergeOTLPAttrs(resourceAttrs, spanAttrs)

				startTime := nanosToTime(span.StartTimeUnixNano)
				endTime := nanosToTime(span.EndTimeUnixNano)
				durationMs := endTime.Sub(startTime).Milliseconds()
				if durationMs < 0 {
					durationMs = 0
				}

				isRoot := span.ParentSpanID == ""

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

				host := firstNonEmpty(spanAttrs["server.address"], spanAttrs["net.host.name"], resourceAttrs["host.name"])
				pod := firstNonEmpty(spanAttrs["k8s.pod.name"], resourceAttrs["k8s.pod.name"])
				container := firstNonEmpty(spanAttrs["k8s.container.name"], resourceAttrs["k8s.container.name"])

				spanKind := spanKindString(span.Kind)

				rootInt := 0
				if isRoot {
					rootInt = 1
				}

				spansToInsert = append(spansToInsert, SpanRecord{
					TeamUUID:       teamUUID,
					TraceID:        span.TraceID,
					SpanID:         span.SpanID,
					ParentSpanID:   span.ParentSpanID,
					IsRoot:         rootInt,
					OperationName:  span.Name,
					ServiceName:    serviceName,
					SpanKind:       spanKind,
					StartTime:      startTime,
					EndTime:        endTime,
					DurationMs:     durationMs,
					Status:         status,
					StatusMessage:  statusMessage,
					HTTPMethod:     httpMethod,
					HTTPURL:        httpURL,
					HTTPStatusCode: httpStatusCode,
					Host:           host,
					Pod:            pod,
					Container:      container,
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
