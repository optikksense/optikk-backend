package rundetail

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) GetRunDetail(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}

	detail, err := h.Service.GetRunDetail(teamID, spanID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get run detail", err)
		return
	}
	if detail == nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "Run not found")
		return
	}
	modulecommon.RespondOK(c, detail)
}

func (h *Handler) GetRunMessages(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}

	messages, err := h.Service.GetRunMessages(teamID, spanID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get run messages", err)
		return
	}
	modulecommon.RespondOK(c, messages)
}

func (h *Handler) GetRunContext(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	traceID := c.Query("traceId")
	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "traceId query param is required")
		return
	}

	ctx, err := h.Service.GetRunContext(teamID, spanID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get run context", err)
		return
	}
	modulecommon.RespondOK(c, ctx)
}
