package detail

import (
	"net/http"
	"strings"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetLogSurrounding(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	logID := strings.TrimSpace(c.Query("id"))
	if logID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "id is required")
		return
	}

	before := modulecommon.ParseIntParam(c, "before", 10)
	after := modulecommon.ParseIntParam(c, "after", 10)
	if before > 100 {
		before = 100
	}
	if after > 100 {
		after = 100
	}

	resp, err := h.Service.GetLogSurrounding(c.Request.Context(), teamID, logID, before, after)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.NotFound, "Log entry not found", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetLogDetail(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Query("traceId")
	spanID := c.Query("spanId")
	timestampMs := modulecommon.ParseInt64Param(c, "timestamp", 0)
	window := modulecommon.ParseIntParam(c, "contextWindow", 30)

	if traceID == "" || spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "traceId and spanId are required")
		return
	}
	if timestampMs == 0 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "timestamp is required")
		return
	}

	centerNs := uint64(timestampMs) * 1_000_000 //nolint:gosec // G115 - domain-constrained value
	windowNs := uint64(window) * 1_000_000_000  //nolint:gosec // G115 - domain-constrained value
	resp, err := h.Service.GetLogDetail(c.Request.Context(), teamID, traceID, spanID, centerNs, centerNs-windowNs, centerNs+windowNs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query log detail", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
