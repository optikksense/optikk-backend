package tracelogs

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

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
		common.RespondError(c, http.StatusBadRequest, errorcode.Validation, "traceId is required")
		return
	}

	resp, err := h.Service.GetTraceLogs(c.Request.Context(), h.GetTenant(c).TeamID, traceID)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query trace logs", err)
		return
	}
	common.RespondOK(c, resp.Logs)
}
