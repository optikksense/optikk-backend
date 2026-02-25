package telemetry

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// HandleLogs is the Gin handler for POST /otlp/v1/logs.
func (h *Handler) HandleLogs(c *gin.Context) {
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

	var payload OTLPLogsPayload
	if isProtobuf(c) {
		payload, err = protoToLogsPayload(body)
	} else {
		err = json.Unmarshal(body, &payload)
	}
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid OTLP payload"})
		return
	}

	var logsToInsert []LogRecord

	for _, rl := range payload.ResourceLogs {
		resourceAttrs := otlpAttrMap(rl.Resource.Attributes)
		defaultService := resourceAttrs["service.name"]
		if defaultService == "" {
			defaultService = "unknown"
		}

		for _, sl := range rl.ScopeLogs {
			for _, record := range sl.LogRecords {
				logAttrs := otlpAttrMap(record.Attributes)
				allAttrs := mergeOTLPAttrs(resourceAttrs, logAttrs)

				ts := nanosToTime(record.TimeUnixNano)
				if strings.TrimSpace(record.TimeUnixNano) == "" {
					ts = nanosToTime(record.ObservedTimeUnixNano)
				}

				serviceName := firstNonEmpty(logAttrs["service.name"], defaultService)
				level := strings.TrimSpace(record.SeverityText)
				if level == "" {
					level = severityTextFromNumber(record.SeverityNumber)
				}
				message := strings.TrimSpace(otlpAttrString(record.Body))
				if message == "" {
					message = strings.TrimSpace(logAttrs["message"])
				}
				logger := firstNonEmpty(logAttrs["logger.name"], sl.Scope.Name)
				exception := firstNonEmpty(logAttrs["exception.message"], logAttrs["exception.type"])
				host := firstNonEmpty(logAttrs["server.address"], logAttrs["net.host.name"], resourceAttrs["host.name"])
				pod := firstNonEmpty(logAttrs["k8s.pod.name"], resourceAttrs["k8s.pod.name"])
				container := firstNonEmpty(logAttrs["k8s.container.name"], resourceAttrs["k8s.container.name"])
				thread := firstNonEmpty(logAttrs["thread.name"], logAttrs["thread.id"])

				logsToInsert = append(logsToInsert, LogRecord{
					TeamUUID:   teamUUID,
					Timestamp:  ts,
					Level:      level,
					Service:    serviceName,
					Logger:     logger,
					Message:    message,
					TraceID:    strings.TrimSpace(record.TraceID),
					SpanID:     strings.TrimSpace(record.SpanID),
					Host:       host,
					Pod:        pod,
					Container:  container,
					Thread:     thread,
					Exception:  exception,
					Attributes: dbutil.JSONString(allAttrs),
				})
			}
		}
	}

	if len(logsToInsert) > 0 {
		if err := h.Ingester.IngestLogs(c.Request.Context(), logsToInsert); err != nil {
			log.Printf("otlp: failed to ingest %d logs: %v", len(logsToInsert), err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ingest logs"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"accepted": len(logsToInsert)})
}
