package tracelogs

import (
	"net/http"

	"github.com/gin-gonic/gin"
	common "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetTraceLogs(c *gin.Context) {
	traceID := c.Param("traceId")
	if traceID == "" {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "traceId is required")
		return
	}

	resp, err := h.Service.GetTraceLogs(c.Request.Context(), h.GetTenant(c).TeamID, traceID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query trace logs")
		return
	}
	common.RespondOK(c, resp.Logs)
}
