package api

import (
	"context"
	"database/sql"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/modules/ingestion/api/decode"
	"github.com/observability/observability-backend-go/modules/ingestion/api/translate"
	"github.com/observability/observability-backend-go/modules/ingestion/ingest"
)

// Handler serves OTLP/HTTP ingestion endpoints.
type Handler struct {
	auth     APIKeyResolver
	ingester ingest.Ingester
}

// NewHandler creates a new telemetry handler.
func NewHandler(ingester ingest.Ingester, mysql *sql.DB) *Handler {
	return &Handler{
		auth:     newMySQLAPIKeyResolver(mysql),
		ingester: ingester,
	}
}

// HandleTraces is the Gin handler for POST /otlp/v1/traces.
func (h *Handler) HandleTraces(c *gin.Context) {
	handleSignal(h, c, decode.ProtoToTracesPayload, translate.Spans, h.ingester.IngestSpans, "spans")
}

// HandleMetrics is the Gin handler for POST /otlp/v1/metrics.
func (h *Handler) HandleMetrics(c *gin.Context) {
	handleSignal(h, c, decode.ProtoToMetricsPayload, translate.Metrics, h.ingester.IngestMetrics, "metrics")
}

// HandleLogs is the Gin handler for POST /otlp/v1/logs.
func (h *Handler) HandleLogs(c *gin.Context) {
	handleSignal(h, c, decode.ProtoToLogsPayload, translate.Logs, h.ingester.IngestLogs, "logs")
}

// handleSignal is the generic handler that eliminates repetition across
// traces, metrics, and logs. It performs: auth → decode → translate → ingest → respond.
func handleSignal[P any, R any](
	h *Handler,
	c *gin.Context,
	protoDec decode.ProtoDecoder[P],
	translator translate.Translator[P, R],
	ingestFn func(ctx context.Context, records []R) error,
	signalName string,
) {
	teamUUID, ok := h.auth.ResolveAPIKey(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing api_key"})
		return
	}

	payload, err := decode.DecodePayload(c, protoDec)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	records := translator.Translate(teamUUID, payload)

	if len(records) > 0 {
		if err := ingestFn(c.Request.Context(), records); err != nil {
			log.Printf("otlp: failed to ingest %d %s: %v", len(records), signalName, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ingest " + signalName})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"accepted": len(records)})
}
