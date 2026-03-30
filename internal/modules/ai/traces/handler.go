package traces

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) GetLLMTrace(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "traceId is required")
		return
	}

	spans, err := h.Service.GetLLMTrace(teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get LLM trace", err)
		return
	}
	modulecommon.RespondOK(c, spans)
}

func (h *Handler) GetLLMTraceSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "traceId is required")
		return
	}

	summary, err := h.Service.GetLLMTraceSummary(teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get LLM trace summary", err)
		return
	}
	modulecommon.RespondOK(c, summary)
}
