package traces

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

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
		common.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "traceId is required")
		return
	}

	spans, err := h.Service.GetLLMTrace(teamID, traceID)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get LLM trace", err)
		return
	}
	common.RespondOK(c, spans)
}

func (h *Handler) GetLLMTraceSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	if traceID == "" {
		common.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "traceId is required")
		return
	}

	summary, err := h.Service.GetLLMTraceSummary(teamID, traceID)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get LLM trace summary", err)
		return
	}
	common.RespondOK(c, summary)
}
