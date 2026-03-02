package telemetry

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	util "github.com/observability/observability-backend-go/internal/helpers"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// IngestCallback is invoked after a successful ingest with the team UUID,
// signal name ("spans", "metrics", "logs"), and the number of accepted records.
// It runs synchronously in the request goroutine, so implementations should
// be fast and non-blocking.
type IngestCallback func(teamUUID string, signal string, count int)

// Handler serves OTLP/HTTP ingestion endpoints.
type Handler struct {
	auth      apiKeyResolver
	ingester  Ingester
	spanCache *SpanCache
	onIngest  IngestCallback
}

func NewHandler(ingester Ingester, mysql *sql.DB) *Handler {
	return &Handler{
		auth:      newCachedAPIKeyResolver(mysql),
		ingester:  ingester,
		spanCache: NewSpanCache(),
	}
}

// SetOnIngest registers a callback that fires after each successful ingest.
func (h *Handler) SetOnIngest(cb IngestCallback) {
	h.onIngest = cb
}

// SpanCacheRef returns the span cache for sharing with the gRPC server.
func (h *Handler) SpanCacheRef() *SpanCache {
	return h.spanCache
}

// Close releases resources held by the Handler (e.g. the span cache purge goroutine).
func (h *Handler) Close() {
	if h.spanCache != nil {
		h.spanCache.Stop()
	}
}

func (h *Handler) HandleTraces(c *gin.Context) {
	teamUUID, ok := h.auth.resolveAPIKey(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing api_key"})
		return
	}

	req, err := DecodeProto(c, func() *coltracepb.ExportTraceServiceRequest {
		return &coltracepb.ExportTraceServiceRequest{}
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	spans := TranslateProtoSpans(teamUUID, req, h.spanCache)
	h.ingestAndRespond(c, spans, h.ingester.IngestSpans, "spans", teamUUID)
}

func (h *Handler) HandleMetrics(c *gin.Context) {
	teamUUID, ok := h.auth.resolveAPIKey(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing api_key"})
		return
	}

	req, err := DecodeProto(c, func() *colmetricspb.ExportMetricsServiceRequest {
		return &colmetricspb.ExportMetricsServiceRequest{}
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	metrics := TranslateProtoMetrics(teamUUID, req)
	h.ingestAndRespond(c, metrics, h.ingester.IngestMetrics, "metrics", teamUUID)
}

func (h *Handler) HandleLogs(c *gin.Context) {
	teamUUID, ok := h.auth.resolveAPIKey(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing api_key"})
		return
	}

	req, err := DecodeProto(c, func() *collogspb.ExportLogsServiceRequest {
		return &collogspb.ExportLogsServiceRequest{}
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	logs := TranslateProtoLogs(teamUUID, req)
	h.ingestAndRespond(c, logs, h.ingester.IngestLogs, "logs", teamUUID)
}

// ingestAndRespond is the generic ingest → respond pipeline for all signals.
func ingestAndRespond[R any](
	h *Handler,
	c *gin.Context,
	records []R,
	ingestFn func(context.Context, []R) (*BatchInsertResult, error),
	signalName string,
	teamUUID string,
) {
	if len(records) == 0 {
		c.JSON(http.StatusOK, gin.H{})
		return
	}

	result, err := ingestFn(c.Request.Context(), records)
	if err != nil && (result == nil || result.AcceptedCount == 0) {
		log.Printf("otlp: failed to ingest %d %s: %v", len(records), signalName, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to ingest " + signalName})
		return
	}

	if result != nil && result.RejectedCount > 0 {
		log.Printf("otlp: partial success for %s: %d accepted, %d rejected: %s",
			signalName, result.AcceptedCount, result.RejectedCount, result.ErrorMessage)

		if h.onIngest != nil && result.AcceptedCount > 0 {
			h.onIngest(teamUUID, signalName, result.AcceptedCount)
		}

		c.JSON(http.StatusOK, gin.H{
			"partialSuccess": gin.H{
				"rejectedItems": result.RejectedCount,
				"errorMessage":  result.ErrorMessage,
			},
		})
		return
	}

	accepted := len(records)
	if result != nil {
		accepted = result.AcceptedCount
	}
	if h.onIngest != nil && accepted > 0 {
		h.onIngest(teamUUID, signalName, accepted)
	}

	c.JSON(http.StatusOK, gin.H{})
}

// Convenience method so HandleTraces/Metrics/Logs can call it as a method.
func (h *Handler) ingestAndRespond(c *gin.Context, records any, ingestFn any, signalName, teamUUID string) {
	// Use type switch to call the right typed version.
	switch r := records.(type) {
	case []SpanRecord:
		fn := ingestFn.(func(context.Context, []SpanRecord) (*BatchInsertResult, error))
		ingestAndRespond(h, c, r, fn, signalName, teamUUID)
	case []MetricRecord:
		fn := ingestFn.(func(context.Context, []MetricRecord) (*BatchInsertResult, error))
		ingestAndRespond(h, c, r, fn, signalName, teamUUID)
	case []LogRecord:
		fn := ingestFn.(func(context.Context, []LogRecord) (*BatchInsertResult, error))
		ingestAndRespond(h, c, r, fn, signalName, teamUUID)
	}
}

// ---------------------------------------------------------------------------
// API key resolver with TTL cache
// ---------------------------------------------------------------------------

type apiKeyResolver interface {
	resolveAPIKey(c *gin.Context) (teamUUID string, ok bool)
}

type cacheEntry struct {
	teamUUID  string
	expiresAt time.Time
}

// cachedAPIKeyResolver validates API keys against MySQL with a 5-minute in-process cache.
type cachedAPIKeyResolver struct {
	db    *sql.DB
	cache sync.Map // map[string]cacheEntry
}

const apiKeyCacheTTL = 5 * time.Minute

func newCachedAPIKeyResolver(db *sql.DB) apiKeyResolver {
	return &cachedAPIKeyResolver{db: db}
}

func (r *cachedAPIKeyResolver) resolveAPIKey(c *gin.Context) (string, bool) {
	apiKey := extractAPIKey(c)
	if apiKey == "" {
		return "", false
	}

	// Fast path: cache hit.
	if v, ok := r.cache.Load(apiKey); ok {
		entry := v.(cacheEntry)
		if time.Now().Before(entry.expiresAt) {
			return entry.teamUUID, true
		}
		r.cache.Delete(apiKey)
	}

	// Slow path: MySQL lookup.
	var teamID int64
	if err := r.db.QueryRow(
		`SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1`, apiKey,
	).Scan(&teamID); err != nil {
		return "", false
	}

	teamUUID := util.ToTeamUUID(teamID)
	r.cache.Store(apiKey, cacheEntry{teamUUID: teamUUID, expiresAt: time.Now().Add(apiKeyCacheTTL)})
	return teamUUID, true
}

func extractAPIKey(c *gin.Context) string {
	if auth := c.GetHeader("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		if key := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")); key != "" {
			return key
		}
	}
	return strings.TrimSpace(c.GetHeader("X-API-Key"))
}
