package detail

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	common "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetLogSurrounding(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	logID := strings.TrimSpace(c.Query("id"))
	if logID == "" {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "id is required")
		return
	}

	before := common.ParseIntParam(c, "before", 10)
	after := common.ParseIntParam(c, "after", 10)
	if before > 100 {
		before = 100
	}
	if after > 100 {
		after = 100
	}

	resp, err := h.Service.GetLogSurrounding(c.Request.Context(), teamID, logID, before, after)
	if err != nil {
		common.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Log entry not found")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetLogDetail(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Query("traceId")
	spanID := c.Query("spanId")
	timestampMs := common.ParseInt64Param(c, "timestamp", 0)
	window := common.ParseIntParam(c, "contextWindow", 30)

	if traceID == "" || spanID == "" {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "traceId and spanId are required")
		return
	}
	if timestampMs == 0 {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "timestamp is required")
		return
	}

	centerNs := uint64(timestampMs) * 1_000_000
	windowNs := uint64(window) * 1_000_000_000
	resp, err := h.Service.GetLogDetail(c.Request.Context(), teamID, traceID, spanID, centerNs, centerNs-windowNs, centerNs+windowNs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log detail")
		return
	}
	common.RespondOK(c, resp)
}
