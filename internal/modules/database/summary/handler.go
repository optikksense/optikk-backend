package summary

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

func (h *Handler) GetSummaryStats(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSummaryStats(c.Request.Context(), teamID, startMs, endMs, shared.ParseFilters(c))
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query database summary stats")
		return
	}
	common.RespondOK(c, resp)
}
