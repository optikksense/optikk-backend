package alerting

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
)

func (h *Handler) CreateChannel(c *gin.Context) {
	tenant := h.getTenant(c)
	var req CreateChannelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}
	channel, err := h.svc.CreateChannel(tenant.TeamID, tenant.UserID, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	c.JSON(http.StatusCreated, channel)
}

func (h *Handler) ListChannels(c *gin.Context) {
	tenant := h.getTenant(c)
	channels, err := h.svc.ListChannels(tenant.TeamID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list channels")
		return
	}
	common.RespondOK(c, channels)
}

func (h *Handler) GetChannelByID(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid channel ID")
		return
	}
	channel, err := h.svc.GetChannelByID(tenant.TeamID, id)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get channel")
		return
	}
	if channel == nil {
		common.RespondError(c, http.StatusNotFound, "NOT_FOUND", "Notification channel not found")
		return
	}
	common.RespondOK(c, channel)
}

func (h *Handler) UpdateChannel(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid channel ID")
		return
	}
	var req UpdateChannelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}
	channel, err := h.svc.UpdateChannel(tenant.TeamID, tenant.UserID, id, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, channel)
}

func (h *Handler) DeleteChannel(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid channel ID")
		return
	}
	if err := h.svc.DeleteChannel(tenant.TeamID, tenant.UserID, id); err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, map[string]string{"status": "deleted"})
}
