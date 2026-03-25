package httpmetrics

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/platform/logger"
	"go.uber.org/zap"
)

type HTTPMetricsHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *HTTPMetricsHandler) GetRequestRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRequestRate(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP request rate", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRequestDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRequestDuration(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP request duration", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetActiveRequests(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetActiveRequests(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query active HTTP requests", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRequestBodySize(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRequestBodySize(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query request body size", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetResponseBodySize(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetResponseBodySize(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query response body size", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetClientDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetClientDuration(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP client duration", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetDNSDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDNSDuration(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query DNS duration", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetTLSDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTLSDuration(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query TLS duration", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetTopRoutesByVolume(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTopRoutesByVolume(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top routes by volume", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetTopRoutesByLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTopRoutesByLatency(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top routes by latency", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRouteErrorRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRouteErrorRate(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query route error rate", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRouteErrorTimeseries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRouteErrorTimeseries(teamID, startMs, endMs)
	if err != nil {
		logger.L().Error("http route error timeseries query failed", zap.Error(err))
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query route error timeseries", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetStatusDistribution(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetStatusDistribution(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP status distribution", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetErrorTimeseries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorTimeseries(teamID, startMs, endMs)
	if err != nil {
		logger.L().Error("http error timeseries query failed", zap.Error(err))
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query HTTP error timeseries", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetTopExternalHosts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTopExternalHosts(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top external hosts", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetExternalHostLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetExternalHostLatency(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query external host latency", err)
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetExternalHostErrorRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetExternalHostErrorRate(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query external host error rate", err)
		return
	}
	RespondOK(c, resp)
}
