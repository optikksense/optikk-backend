package explorer

import (
	"net/http"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
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

// Query powers POST /api/v1/logs/query (list + optional include blocks).
func (h *Handler) Query(c *gin.Context) {
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if !validRange(req.StartTime, req.EndTime) {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}
	resp, err := h.svc.Query(c.Request.Context(), req, h.GetTenant(c).TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query logs", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

// Analytics powers POST /api/v1/logs/analytics.
func (h *Handler) Analytics(c *gin.Context) {
	var req AnalyticsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if !validRange(req.StartTime, req.EndTime) {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}
	resp, err := h.svc.Analytics(c.Request.Context(), req, h.GetTenant(c).TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query logs analytics", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

// GetByID powers GET /api/v1/logs/:id (single-log deep link). The `id`
// segment is `<trace_id>:<span_id>:<timestamp_ns>` base64url.
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

func validRange(start, end int64) bool {
	return start > 0 && end > 0 && start < end
}
