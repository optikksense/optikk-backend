package trace_paths //nolint:revive,stylecheck

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
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

func (h *Handler) GetCriticalPath(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	path, err := h.svc.GetCriticalPath(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to compute critical path", err)
		return
	}
	modulecommon.RespondOK(c, path)
}

func (h *Handler) GetErrorPath(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	path, err := h.svc.GetErrorPath(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to compute error path", err)
		return
	}
	modulecommon.RespondOK(c, path)
}
