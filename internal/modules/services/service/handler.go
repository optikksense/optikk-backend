package servicepage

import (
	"context"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type ServiceHandler struct {
	modulecommon.DBTenant
	Service *Service
}

type CountResponse struct {
	Count int64 `json:"count"`
}

func (h *ServiceHandler) GetTotalServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetTotalServices, "Failed to query total services")
}

func (h *ServiceHandler) GetHealthyServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetHealthyServices, "Failed to query healthy services")
}

func (h *ServiceHandler) GetDegradedServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetDegradedServices, "Failed to query degraded services")
}

func (h *ServiceHandler) GetUnhealthyServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetUnhealthyServices, "Failed to query unhealthy services")
}

func (h *ServiceHandler) GetServiceMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetServiceMetrics(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query service metrics", err)
		return
	}

	modulecommon.RespondOK(c, rows)
}

func (h *ServiceHandler) GetServiceTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetServiceTimeSeries(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query service timeseries", err)
		return
	}

	modulecommon.RespondOK(c, points)
}

func (h *ServiceHandler) GetServiceEndpoints(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Param("serviceName")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	endpoints, err := h.Service.GetServiceEndpoints(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query endpoint breakdown", err)
		return
	}

	modulecommon.RespondOK(c, endpoints)
}

func (h *ServiceHandler) GetSpanAnalysis(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Param("serviceName")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetSpanAnalysis(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span analysis", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *ServiceHandler) GetServiceInfraMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Param("serviceName")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	metrics, err := h.Service.GetServiceInfraMetrics(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query service infrastructure metrics", err)
		return
	}
	modulecommon.RespondOK(c, metrics)
}

func (h *ServiceHandler) respondWithCount(c *gin.Context, fn func(context.Context, int64, int64, int64) (int64, error), message string) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	count, err := fn(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, message, err)
		return
	}

	modulecommon.RespondOK(c, CountResponse{Count: count})
}
