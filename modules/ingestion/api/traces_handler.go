package api

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// HandleTraces is the Gin handler for POST /otlp/v1/traces.
func (h *Handler) HandleTraces(c *gin.Context) {
	teamUUID, payload, ok := decodeRequest(h, c, protoToTracesPayload)
	if !ok {
		return
	}

	spansToInsert := TranslateSpans(teamUUID, payload)

	if len(spansToInsert) > 0 {
		if err := h.Ingester.IngestSpans(c.Request.Context(), spansToInsert); err != nil {
			log.Printf("otlp: failed to ingest %d spans: %v", len(spansToInsert), err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ingest spans"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"accepted": len(spansToInsert)})
}
