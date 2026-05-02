package log_trends //nolint:revive,stylecheck

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
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

// Summary powers POST /api/v1/logs/summary — total / errors / warns counts.
func (h *Handler) Summary(c *gin.Context) {
	f, ok := h.bindFilters(c)
	if !ok {
		return
	}
	sum, err := h.svc.Summary(c.Request.Context(), f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query logs summary", err)
		return
	}
	modulecommon.RespondOK(c, SummaryResponse{Summary: sum})
}

// Trend powers POST /api/v1/logs/trend — severity-bucketed time series at
// display grain.
func (h *Handler) Trend(c *gin.Context) {
	f, ok := h.bindFilters(c)
	if !ok {
		return
	}
	tr, err := h.svc.Trend(c.Request.Context(), f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query logs trend", err)
		return
	}
	modulecommon.RespondOK(c, TrendResponse{Trend: tr})
}

func (h *Handler) bindFilters(c *gin.Context) (filter.Filters, bool) {
	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return filter.Filters{}, false
	}
	req.Filters.TeamID = h.GetTenant(c).TeamID
	req.Filters.StartMs = req.StartTime
	req.Filters.EndMs = req.EndTime
	if err := req.Filters.Validate(); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "Invalid filters", err)
		return filter.Filters{}, false
	}
	return req.Filters, true
}
