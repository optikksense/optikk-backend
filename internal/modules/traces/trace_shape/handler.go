package trace_shape //nolint:revive,stylecheck

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

func (h *Handler) GetSpanKindBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	b, err := h.svc.GetSpanKindBreakdown(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span kind breakdown", err)
		return
	}
	modulecommon.RespondOK(c, b)
}

func (h *Handler) GetFlamegraphData(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	f, err := h.svc.GetFlamegraphData(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch flamegraph", err)
		return
	}
	modulecommon.RespondOK(c, f)
}
