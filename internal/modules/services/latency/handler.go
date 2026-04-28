package latency

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	svc *Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, svc *Service) *Handler {
	return &Handler{DBTenant: modulecommon.DBTenant{GetTenant: getTenant}, svc: svc}
}

func (h *Handler) Histogram(c *gin.Context) {
	var req HistogramRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query params")
		return
	}
	resp, err := h.svc.Histogram(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch histogram", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) Heatmap(c *gin.Context) {
	var req HeatmapRequest
	if err := c.ShouldBindQuery(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query params")
		return
	}
	resp, err := h.svc.Heatmap(c.Request.Context(), h.GetTenant(c).TeamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch heatmap", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
