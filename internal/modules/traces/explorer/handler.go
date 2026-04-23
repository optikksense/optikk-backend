package explorer

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	svc *Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, svc *Service) *Handler {
	return &Handler{DBTenant: modulecommon.DBTenant{GetTenant: getTenant}, svc: svc}
}

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
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query traces", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetByID(c *gin.Context) {
	traceID := c.Param("traceId")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "trace id required")
		return
	}
	resp, err := h.svc.GetByID(c.Request.Context(), h.GetTenant(c).TeamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch trace", err)
		return
	}
	if resp == nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "trace not found")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func validRange(start, end int64) bool { return start > 0 && end > 0 && start < end }
