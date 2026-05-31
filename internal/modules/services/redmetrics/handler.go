package redmetrics

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/infra/cursor"
	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type REDMetricsHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *REDMetricsHandler) GetSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSummary(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query RED summary", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetApdex(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	satisfiedMs := modulecommon.ParseFloatParam(c, "satisfied_ms", 300.0)
	toleratingMs := modulecommon.ParseFloatParam(c, "tolerating_ms", 1200.0)
	if satisfiedMs <= 0 || toleratingMs <= 0 || satisfiedMs >= toleratingMs {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "satisfied_ms must be positive and less than tolerating_ms")
		return
	}
	serviceName := c.Query("serviceName")
	resp, err := h.Service.GetApdex(c.Request.Context(), teamID, startMs, endMs, satisfiedMs, toleratingMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query Apdex scores", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetRequestRateTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetRequestRateTimeSeries(c.Request.Context(), teamID, s, e)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query request rate time series", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetP95LatencyTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetP95LatencyTimeSeries(c.Request.Context(), teamID, s, e)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query p95 latency time series", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetServiceSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Param("serviceName")
	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetServiceSummary(c.Request.Context(), teamID, s, e, serviceName)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query service summary", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *REDMetricsHandler) GetSaturationTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Param("serviceName")
	resp, err := h.Service.GetSaturationTimeSeries(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query saturation time series", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

// GetStatusTimeSeries returns rps split by 2xx/4xx/5xx over time for a service.
func (h *REDMetricsHandler) GetStatusTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	resp, err := h.Service.GetStatusTimeSeries(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query status time series", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

// GetLatencyPercentilesTimeSeries returns p50/p95/p99 over time for a service.
func (h *REDMetricsHandler) GetLatencyPercentilesTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	resp, err := h.Service.GetLatencyPercentilesTimeSeries(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency percentiles", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

// GetOperationBaseline returns windowed p50/p95/p99 for one service+operation,
// powering the Trace Detail Duration "N× slower than p50" comparison.
func (h *REDMetricsHandler) GetOperationBaseline(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("service")
	operationName := c.Query("operation")
	if serviceName == "" || operationName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "service and operation are required")
		return
	}
	resp, err := h.Service.GetOperationBaseline(c.Request.Context(), teamID, startMs, endMs, serviceName, operationName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query operation baseline", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

// GetTopEndpointsCombined returns per-operation rate/err/percentiles for the
// Service Detail endpoints table. Primary + comparison payloads are returned
// when `compareTo=previous_period` is set, letting the FE compute p99 delta.
func (h *REDMetricsHandler) GetTopEndpointsCombined(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	limit := modulecommon.ParsePageSize(c, "limit", 50)
	cursorStr := c.Query("cursor")
	var cur TopEndpointsCursor
	if cursorStr != "" {
		if decoded, ok := cursor.Decode[TopEndpointsCursor](cursorStr); ok {
			cur = decoded
		}
	}
	resp, err := modulecommon.WithComparison(c, startMs, endMs, func(s, e int64) (any, error) {
		return h.Service.GetTopEndpointsCombined(c.Request.Context(), teamID, s, e, serviceName, limit, cur)
	})
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top endpoints", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
