package metricsexplorer

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	GetTenant modulecommon.GetTenantFunc
	Service   Service
}

func (h *Handler) ListMetricNames(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	search := c.DefaultQuery("search", "")

	results, err := h.Service.ListMetricNames(c.Request.Context(), teamID, startMs, endMs, search)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list metric names", err)
		return
	}
	modulecommon.RespondOK(c, results)
}

func (h *Handler) ListTagKeys(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	metricName := c.Query("metricName")
	if metricName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "metricName is required")
		return
	}

	results, err := h.Service.ListTagKeys(c.Request.Context(), teamID, startMs, endMs, metricName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list tag keys", err)
		return
	}
	modulecommon.RespondOK(c, results)
}

func (h *Handler) ListTagValues(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	metricName := c.Query("metricName")
	if metricName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "metricName is required")
		return
	}
	tagKey := c.Query("tagKey")
	if tagKey == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "tagKey is required")
		return
	}

	results, err := h.Service.ListTagValues(c.Request.Context(), teamID, startMs, endMs, metricName, tagKey)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list tag values", err)
		return
	}
	modulecommon.RespondOK(c, results)
}

func (h *Handler) Query(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	var req ExplorerQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid request body")
		return
	}
	if len(req.Queries) == 0 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "At least one query is required")
		return
	}

	result, err := h.Service.Query(c.Request.Context(), teamID, startMs, endMs, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to execute explorer query", err)
		return
	}
	modulecommon.RespondOK(c, result)
}
