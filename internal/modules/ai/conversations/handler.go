package conversations

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) ListConversations(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}

	limit := 50
	if v := c.Query("limit"); v != "" {
		limit, _ = strconv.Atoi(v)
	}

	convos, err := h.Service.ListConversations(c.Request.Context(), teamID, startMs, endMs, limit)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list conversations")
		return
	}
	common.RespondOK(c, convos)
}

func (h *Handler) GetConversation(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	conversationID := c.Param("conversationId")
	if conversationID == "" {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "conversationId is required")
		return
	}
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}

	turns, err := h.Service.GetConversation(c.Request.Context(), teamID, conversationID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get conversation")
		return
	}
	common.RespondOK(c, turns)
}
