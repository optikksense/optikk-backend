package trace_servicemap //nolint:revive,stylecheck

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	svc Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, svc Service) *Handler {
	return &Handler{DBTenant: modulecommon.DBTenant{GetTenant: getTenant}, svc: svc}
}

func (h *Handler) GetServiceMap(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	resp, err := h.svc.GetServiceMap(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to compute service map", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTraceErrors(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	groups, err := h.svc.GetTraceErrors(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch trace errors", err)
		return
	}
	modulecommon.RespondOK(c, groups)
}
