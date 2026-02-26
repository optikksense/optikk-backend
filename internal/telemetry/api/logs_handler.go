package api

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// HandleLogs is the Gin handler for POST /otlp/v1/logs.
func (h *Handler) HandleLogs(c *gin.Context) {
	teamUUID, payload, ok := decodeRequest(h, c, protoToLogsPayload)
	if !ok {
		return
	}

	logsToInsert := TranslateLogs(teamUUID, payload)

	if len(logsToInsert) > 0 {
		if err := h.Ingester.IngestLogs(c.Request.Context(), logsToInsert); err != nil {
			log.Printf("otlp: failed to ingest %d logs: %v", len(logsToInsert), err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ingest logs"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"accepted": len(logsToInsert)})
}
