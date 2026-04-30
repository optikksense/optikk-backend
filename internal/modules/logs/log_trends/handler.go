package log_trends //nolint:revive,stylecheck

import (
	"log/slog"
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

// Trends powers POST /api/v1/logs/trends.
func (h *Handler) Trends(c *gin.Context) {
	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	req.Filters.TeamID = h.GetTenant(c).TeamID
	req.Filters.StartMs = req.StartTime
	req.Filters.EndMs = req.EndTime
	if err := req.Filters.Validate(); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Invalid filters", err)
		return
	}
	resp, err := h.svc.ComputeResponse(c.Request.Context(), req.Filters)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query logs trends", err)
		return
	}
	slog.Debug("Logs trends queried successfully", slog.Any("resp", resp))
	modulecommon.RespondOK(c, resp)
}
