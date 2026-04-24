package span_query //nolint:revive,stylecheck

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

func (h *Handler) Query(c *gin.Context) {
	var req SpansQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}
	resp, err := h.svc.Query(c.Request.Context(), req, h.GetTenant(c).TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query spans", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
