package conversations

import (
	"net/http"
	"strconv"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) ListConversations(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	limit := 50
	if v := c.Query("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}

	convos, err := h.Service.ListConversations(c.Request.Context(), teamID, startMs, endMs, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list conversations", err)
		return
	}
	modulecommon.RespondOK(c, convos)
}

func (h *Handler) GetConversation(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	conversationID := c.Param("conversationId")
	if conversationID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "conversationId is required")
		return
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	turns, err := h.Service.GetConversation(c.Request.Context(), teamID, conversationID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get conversation", err)
		return
	}
	modulecommon.RespondOK(c, turns)
}
