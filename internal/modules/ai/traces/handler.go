package traces

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) GetLLMTrace(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	if traceID == "" {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "traceId is required")
		return
	}

	spans, err := h.Service.GetLLMTrace(teamID, traceID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get LLM trace")
		return
	}
	common.RespondOK(c, spans)
}

func (h *Handler) GetLLMTraceSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	if traceID == "" {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "traceId is required")
		return
	}

	summary, err := h.Service.GetLLMTraceSummary(teamID, traceID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get LLM trace summary")
		return
	}
	common.RespondOK(c, summary)
}
