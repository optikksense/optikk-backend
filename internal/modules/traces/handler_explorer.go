package traces

import (
	"net/http"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

func (h *Handler) Query(c *gin.Context) {
	var req QueryRequest
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
	resp, err := h.svc.Query(c.Request.Context(), req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query traces", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) QueryFacets(c *gin.Context) {
	var req FacetsRequest
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
	resp, err := h.svc.QueryFacets(c.Request.Context(), req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query trace facets", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) QueryTrend(c *gin.Context) {
	var req TrendRequest
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
	resp, err := h.svc.QueryTrend(c.Request.Context(), req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query trace trend", err)
		return
	}
	modulecommon.RespondOK(c, resp)
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
