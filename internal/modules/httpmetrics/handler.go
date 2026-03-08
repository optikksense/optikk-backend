package httpmetrics

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// HTTPMetricsHandler handles API endpoints for HTTP metrics.
type HTTPMetricsHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *HTTPMetricsHandler) GetRequestRate(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRequestRate(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query HTTP request rate")
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRequestDuration(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRequestDuration(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query HTTP request duration")
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetActiveRequests(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetActiveRequests(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query active HTTP requests")
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetRequestBodySize(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetRequestBodySize(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query request body size")
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetResponseBodySize(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetResponseBodySize(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query response body size")
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetClientDuration(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetClientDuration(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query HTTP client duration")
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetDNSDuration(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetDNSDuration(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query DNS duration")
		return
	}
	RespondOK(c, resp)
}

func (h *HTTPMetricsHandler) GetTLSDuration(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTLSDuration(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query TLS duration")
		return
	}
	RespondOK(c, resp)
}
