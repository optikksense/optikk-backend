package logdetail

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
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		svc:      svc,
	}
}

// GetByID powers GET /api/v1/logs/:id (single-log deep link).
func (h *Handler) GetByID(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "log id required")
		return
	}
	resp, err := h.svc.GetByID(c.Request.Context(), h.GetTenant(c).TeamID, id)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch log", err)
		return
	}
	if resp == nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "log not found")
		return
	}
	modulecommon.RespondOK(c, resp)
}
