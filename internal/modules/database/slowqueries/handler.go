package slowqueries

import (
	"net/http"

	"github.com/gin-gonic/gin"
	common "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	shared "github.com/observability/observability-backend-go/internal/modules/database/internal/shared"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetSlowQueryPatterns(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSlowQueryPatterns(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c), shared.ParseLimit(c, 20))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query slow query patterns")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetSlowestCollections(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSlowestCollections(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query slowest collections")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetSlowQueryRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSlowQueryRate(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c), shared.ParseThreshold(c, 100))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query slow query rate")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetP99ByQueryText(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetP99ByQueryText(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c), shared.ParseLimit(c, 10))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query p99 by query text")
		return
	}
	common.RespondOK(c, resp)
}
