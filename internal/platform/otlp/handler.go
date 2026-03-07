package otlp

import (
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/platform/ingest"
	"github.com/observability/observability-backend-go/internal/platform/otlp/auth"
	otlpgrpc "github.com/observability/observability-backend-go/internal/platform/otlp/grpc"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// Handler holds the ingest queues and an Authenticator for API keys.
type Handler struct {
	auth           *auth.Authenticator
	resourcesQueue *ingest.Queue
	spansQueue     *ingest.Queue
	logsQueue      *ingest.Queue
	metricsQueue   *ingest.Queue
}

// NewHandler creates an OTLP HTTP handler.
func NewHandler(
	auth *auth.Authenticator,
	resourcesQueue *ingest.Queue,
	spansQueue *ingest.Queue,
	logsQueue *ingest.Queue,
	metricsQueue *ingest.Queue,
) *Handler {
	return &Handler{
		auth:           auth,
		resourcesQueue: resourcesQueue,
		spansQueue:     spansQueue,
		logsQueue:      logsQueue,
		metricsQueue:   metricsQueue,
	}
}

// RegisterRoutes registers the OTLP/HTTP endpoints on the given router group.
func (h *Handler) RegisterRoutes(r *gin.RouterGroup) {
	r.POST("/v1/traces", h.ExportTraces)
	r.POST("/v1/logs", h.ExportLogs)
	r.POST("/v1/metrics", h.ExportMetrics)
}

// resolveTeamID looks up team_id from the X-API-Key header sent by the SDK.
func (h *Handler) resolveTeamID(c *gin.Context) (string, bool) {
	apiKey := c.GetHeader("X-API-Key")
	teamID, err := h.auth.ResolveTeamID(c.Request.Context(), apiKey)

	if err != nil {
		if errors.Is(err, auth.ErrMissingAPIKey) || errors.Is(err, auth.ErrInvalidAPIKey) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return "", false
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return "", false
	}

	return teamID, true
}

// ExportTraces accepts OTLP/HTTP JSON trace payloads.
// POST /otlp/v1/traces
func (h *Handler) ExportTraces(c *gin.Context) {
	teamID, ok := h.resolveTeamID(c)
	if !ok {
		return
	}

	var (
		spanRows     []ingest.Row
		resourceRows []ingest.Row
	)
	contentType := c.ContentType()

	if contentType == "application/x-protobuf" {
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read protobuf body"})
			return
		}
		var protoReq tracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &protoReq); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to decode protobuf traces"})
			return
		}
		mapped := otlpgrpc.MapTraceRows(teamID, &protoReq)
		spanRows = mapped.Spans
		resourceRows = mapped.Resources
	} else {
		var req ExportTraceServiceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid OTLP traces payload: " + err.Error()})
			return
		}
		mapped := MapTraceRows(teamID, req)
		spanRows = mapped.Spans
		resourceRows = mapped.Resources
	}

	if len(resourceRows) == 0 && len(spanRows) == 0 {
		c.Status(http.StatusAccepted)
		return
	}

	if len(resourceRows) > 0 {
		if err := h.resourcesQueue.Enqueue(resourceRows); err != nil {
			if err == ingest.ErrBackpressure {
				c.Header("Retry-After", "5")
				c.JSON(http.StatusTooManyRequests, gin.H{"error": "ingest queue full, retry after 5s"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue resources"})
			return
		}
	}

	if len(spanRows) > 0 {
		if err := h.spansQueue.Enqueue(spanRows); err != nil {
			if err == ingest.ErrBackpressure {
				c.Header("Retry-After", "5")
				c.JSON(http.StatusTooManyRequests, gin.H{"error": "ingest queue full, retry after 5s"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue spans"})
			return
		}
	}

	c.Status(http.StatusAccepted)
}

// ExportLogs accepts OTLP/HTTP JSON log payloads.
// POST /otlp/v1/logs
func (h *Handler) ExportLogs(c *gin.Context) {
	teamID, ok := h.resolveTeamID(c)
	if !ok {
		return
	}

	var rows []ingest.Row
	contentType := c.ContentType()

	if contentType == "application/x-protobuf" {
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read protobuf body"})
			return
		}
		var protoReq logspb.ExportLogsServiceRequest
		if err := proto.Unmarshal(body, &protoReq); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to decode protobuf logs"})
			return
		}
		rows = otlpgrpc.MapLogs(teamID, &protoReq)
	} else {
		var req ExportLogsServiceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid OTLP logs payload: " + err.Error()})
			return
		}
		rows = MapLogs(teamID, req)
	}

	if len(rows) == 0 {
		c.Status(http.StatusAccepted)
		return
	}

	if err := h.logsQueue.Enqueue(rows); err != nil {
		if err == ingest.ErrBackpressure {
			c.Header("Retry-After", "5")
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "ingest queue full, retry after 5s"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue logs"})
		return
	}

	c.Status(http.StatusAccepted)
}

// ExportMetrics accepts OTLP/HTTP JSON metric payloads.
// POST /otlp/v1/metrics
func (h *Handler) ExportMetrics(c *gin.Context) {
	teamID, ok := h.resolveTeamID(c)
	if !ok {
		return
	}

	var rows []ingest.Row
	contentType := c.ContentType()

	if contentType == "application/x-protobuf" {
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read protobuf body"})
			return
		}
		var protoReq metricspb.ExportMetricsServiceRequest
		if err := proto.Unmarshal(body, &protoReq); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "failed to decode protobuf metrics"})
			return
		}
		rows = otlpgrpc.MapMetrics(teamID, &protoReq)
	} else {
		var req ExportMetricsServiceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid OTLP metrics payload: " + err.Error()})
			return
		}
		rows = MapMetrics(teamID, req)
	}

	if len(rows) == 0 {
		c.Status(http.StatusAccepted)
		return
	}

	if err := h.metricsQueue.Enqueue(rows); err != nil {
		if err == ingest.ErrBackpressure {
			c.Header("Retry-After", "5")
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "ingest queue full, retry after 5s"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue metrics"})
		return
	}

	c.Status(http.StatusAccepted)
}
