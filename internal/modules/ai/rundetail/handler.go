package rundetail

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

func (h *Handler) GetRunDetail(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	if spanID == "" {
		common.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}

	detail, err := h.Service.GetRunDetail(teamID, spanID)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get run detail", err)
		return
	}
	if detail == nil {
		common.RespondError(c, http.StatusNotFound, errorcode.NotFound, "Run not found")
		return
	}
	common.RespondOK(c, detail)
}

func (h *Handler) GetRunMessages(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	if spanID == "" {
		common.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}

	messages, err := h.Service.GetRunMessages(teamID, spanID)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get run messages", err)
		return
	}
	common.RespondOK(c, messages)
}

func (h *Handler) GetRunContext(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	traceID := c.Query("traceId")
	if spanID == "" {
		common.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}
	if traceID == "" {
		common.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "traceId query param is required")
		return
	}

	ctx, err := h.Service.GetRunContext(teamID, spanID, traceID)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get run context", err)
		return
	}
	common.RespondOK(c, ctx)
}
