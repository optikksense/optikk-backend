package otlp

import (
	"errors"
	"io"
	"log"
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

type Handler struct {
	auth         *auth.Authenticator
	spansQueue   *ingest.Queue
	logsQueue    *ingest.Queue
	metricsQueue *ingest.Queue
	tracker      *ingest.ByteTracker
}

func NewHandler(
	auth *auth.Authenticator,
	spansQueue *ingest.Queue,
	logsQueue *ingest.Queue,
	metricsQueue *ingest.Queue,
	tracker *ingest.ByteTracker,
) *Handler {
	return &Handler{
		auth:         auth,
		spansQueue:   spansQueue,
		logsQueue:    logsQueue,
		metricsQueue: metricsQueue,
		tracker:      tracker,
	}
}

const maxRequestBodySize = 50 * 1024 * 1024

func (h *Handler) RegisterRoutes(r *gin.RouterGroup) {
	r.POST("/v1/traces", h.ExportTraces)
	r.POST("/v1/logs", h.ExportLogs)
	r.POST("/v1/metrics", h.ExportMetrics)
}

// limitBody wraps the request body with a size limit to prevent OOM from oversized payloads.
func limitBody(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxRequestBodySize)
}

// resolveTeamID looks up team_id from the X-API-Key header sent by the SDK.
func (h *Handler) resolveTeamID(c *gin.Context) (int64, bool) {
	apiKey := c.GetHeader("X-API-Key")
	teamID, err := h.auth.ResolveTeamID(c.Request.Context(), apiKey)

	if err != nil {
		if errors.Is(err, auth.ErrMissingAPIKey) || errors.Is(err, auth.ErrInvalidAPIKey) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return 0, false
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return 0, false
	}

	return teamID, true
}

// recordIngestionSize asynchronously updates the data_ingested_kb column for the team.
func (h *Handler) recordIngestionSize(teamID int64, size int64) {
	if size <= 0 || h.tracker == nil || teamID <= 0 {
		return
	}
	h.tracker.Track(teamID, size)
}

// ExportTraces accepts OTLP/HTTP JSON trace payloads.
func (h *Handler) ExportTraces(c *gin.Context) {
	limitBody(c)
	teamID, ok := h.resolveTeamID(c)
	if !ok {
		return
	}

	var spanRows []ingest.Row
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
		spanRows = otlpgrpc.MapSpans(teamID, &protoReq)
	} else {
		var req ExportTraceServiceRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid OTLP traces payload: " + err.Error()})
			return
		}
		spanRows = MapSpans(teamID, req)
	}

	if len(spanRows) == 0 {
		c.Status(http.StatusAccepted)
		return
	}

	if len(spanRows) > 0 {
		log.Printf("ingest: received %d spans via HTTP for team %d", len(spanRows), teamID)
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

	h.recordIngestionSize(teamID, c.Request.ContentLength)
	c.Status(http.StatusAccepted)
}

// ExportLogs accepts OTLP/HTTP JSON log payloads.
func (h *Handler) ExportLogs(c *gin.Context) {
	limitBody(c)
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

	log.Printf("ingest: received %d logs via HTTP for team %d", len(rows), teamID)
	if err := h.logsQueue.Enqueue(rows); err != nil {
		if err == ingest.ErrBackpressure {
			c.Header("Retry-After", "5")
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "ingest queue full, retry after 5s"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue logs"})
		return
	}

	h.recordIngestionSize(teamID, c.Request.ContentLength)
	c.Status(http.StatusAccepted)
}

// ExportMetrics accepts OTLP/HTTP JSON metric payloads.
func (h *Handler) ExportMetrics(c *gin.Context) {
	limitBody(c)
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

	log.Printf("ingest: received %d metrics via HTTP for team %d", len(rows), teamID)
	if err := h.metricsQueue.Enqueue(rows); err != nil {
		if err == ingest.ErrBackpressure {
			c.Header("Retry-After", "5")
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "ingest queue full, retry after 5s"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue metrics"})
		return
	}

	h.recordIngestionSize(teamID, c.Request.ContentLength)
	c.Status(http.StatusAccepted)
}
