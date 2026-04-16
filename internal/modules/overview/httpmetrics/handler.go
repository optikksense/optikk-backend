package httpmetrics

import (
	"log/slog"
	"net/http"

	errorcode "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type HTTPMetricsHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *HTTPMetricsHandler) GetRequestRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRequestRate(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP request rate", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRequestDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRequestDuration(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP request duration", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetActiveRequests(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetActiveRequests(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query active HTTP requests", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRequestBodySize(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRequestBodySize(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query request body size", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetResponseBodySize(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetResponseBodySize(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query response body size", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetClientDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetClientDuration(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP client duration", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetDNSDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDNSDuration(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query DNS duration", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetTLSDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTLSDuration(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query TLS duration", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetTopRoutesByVolume(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTopRoutesByVolume(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top routes by volume", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetTopRoutesByLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTopRoutesByLatency(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top routes by latency", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRouteErrorRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRouteErrorRate(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query route error rate", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRouteErrorTimeseries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRouteErrorTimeseries(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		slog.Error("http route error timeseries query failed", slog.Any("error", err))
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query route error timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetStatusDistribution(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetStatusDistribution(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP status distribution", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetErrorTimeseries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorTimeseries(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		slog.Error("http error timeseries query failed", slog.Any("error", err))
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP error timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetTopExternalHosts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTopExternalHosts(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top external hosts", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetExternalHostLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetExternalHostLatency(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query external host latency", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetExternalHostErrorRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetExternalHostErrorRate(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query external host error rate", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
