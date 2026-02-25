package api

import (
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/telemetry/model"
)

// HandleLogs is the Gin handler for POST /otlp/v1/logs.
func (h *Handler) HandleLogs(c *gin.Context) {
	teamUUID, payload, ok := decodeRequest(h, c, protoToLogsPayload)
	if !ok {
		return
	}

	var logsToInsert []model.LogRecord

	for _, rl := range payload.ResourceLogs {
		rc := newResourceContext(rl.Resource.Attributes)

		for _, sl := range rl.ScopeLogs {
			for _, record := range sl.LogRecords {
				logAttrs := otlpAttrMap(record.Attributes)
				allAttrs := mergeOTLPAttrs(rc.attrs, logAttrs)

				ts := nanosToTime(record.TimeUnixNano)
				if strings.TrimSpace(record.TimeUnixNano) == "" {
					ts = nanosToTime(record.ObservedTimeUnixNano)
				}

				level := strings.TrimSpace(record.SeverityText)
				if level == "" {
					level = severityTextFromNumber(record.SeverityNumber)
				}

				message := strings.TrimSpace(otlpAttrString(record.Body))
				if message == "" {
					message = strings.TrimSpace(logAttrs["message"])
				}

				infra := extractInfraLabels(logAttrs, rc.attrs)

				logsToInsert = append(logsToInsert, model.LogRecord{
					TeamUUID:   teamUUID,
					Timestamp:  ts,
					Level:      level,
					Service:    firstNonEmpty(logAttrs["service.name"], rc.serviceName),
					Logger:     firstNonEmpty(logAttrs["logger.name"], sl.Scope.Name),
					Message:    message,
					TraceID:    strings.TrimSpace(record.TraceID),
					SpanID:     strings.TrimSpace(record.SpanID),
					Host:       infra.host,
					Pod:        infra.pod,
					Container:  infra.container,
					Thread:     firstNonEmpty(logAttrs["thread.name"], logAttrs["thread.id"]),
					Exception:  firstNonEmpty(logAttrs["exception.message"], logAttrs["exception.type"]),
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
