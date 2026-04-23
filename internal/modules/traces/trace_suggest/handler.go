package trace_suggest //nolint:revive,stylecheck

import (
	"net/http"
	"strings"

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

func (h *Handler) Suggest(c *gin.Context) {
	var req SuggestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}
	req.Field = strings.TrimSpace(req.Field)
	if req.Field == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "field is required")
		return
	}
	if !strings.HasPrefix(req.Field, "@") && !IsScalarField(req.Field) {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "unknown field")
		return
	}
	resp, err := h.svc.Suggest(c.Request.Context(), req, h.GetTenant(c).TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch suggestions", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
