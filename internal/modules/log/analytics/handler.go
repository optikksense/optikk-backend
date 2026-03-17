package analytics

import (
	"net/http"

	"github.com/gin-gonic/gin"
	common "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	shared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetLogHistogram(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	resp, err := h.Service.GetLogHistogram(c.Request.Context(), filters, c.Query("step"))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log histogram")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLogVolume(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	resp, err := h.Service.GetLogVolume(c.Request.Context(), filters, c.Query("step"))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log volume")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLogStats(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	resp, err := h.Service.GetLogStats(c.Request.Context(), filters)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log stats")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLogFields(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	resp, err := h.Service.GetLogFields(c.Request.Context(), filters, c.Query("field"))
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLogAggregate(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	var req LogAggregateRequest
	_ = c.ShouldBindQuery(&req)
	resp, err := h.Service.GetLogAggregate(c.Request.Context(), filters, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, resp)
}
