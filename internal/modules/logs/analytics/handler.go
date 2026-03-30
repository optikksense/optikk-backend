package analytics

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
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
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query log histogram", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetLogVolume(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	resp, err := h.Service.GetLogVolume(c.Request.Context(), filters, c.Query("step"))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query log volume", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetLogStats(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	resp, err := h.Service.GetLogStats(c.Request.Context(), filters)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query log stats", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetLogFields(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	resp, err := h.Service.GetLogFields(c.Request.Context(), filters, c.Query("field"))
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetLogAggregate(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}
	var req LogAggregateRequest
	_ = c.ShouldBindQuery(&req)
	if _, err := buildAggregateQuery(req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	resp, err := h.Service.GetLogAggregate(c.Request.Context(), filters, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query log aggregate", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
