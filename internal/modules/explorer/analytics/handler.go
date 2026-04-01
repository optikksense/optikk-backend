package analytics

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler handles HTTP requests for the unified analytics endpoint.
type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

// NewHandler creates a new analytics handler.
func NewHandler(getTenant modulecommon.GetTenantFunc, service *Service) *Handler {
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  service,
	}
}

// Query handles POST /v1/explorer/:scope/analytics
func (h *Handler) Query(c *gin.Context) {
	scope := c.Param("scope")
	if scope != "logs" && scope != "traces" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "scope must be 'logs' or 'traces'")
		return
	}

	var req AnalyticsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}

	result, err := h.Service.RunQuery(c.Request.Context(), h.GetTenant(c).TeamID, req, scope)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Analytics query failed", err)
		return
	}
	modulecommon.RespondOK(c, result)
}
