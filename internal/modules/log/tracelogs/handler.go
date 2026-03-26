package tracelogs

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetTraceLogs(c *gin.Context) {
	traceID := c.Param("traceId")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "traceId is required")
		return
	}

	resp, err := h.Service.GetTraceLogs(c.Request.Context(), h.GetTenant(c).TeamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query trace logs", err)
		return
	}
	modulecommon.RespondOK(c, resp.Logs)
}
