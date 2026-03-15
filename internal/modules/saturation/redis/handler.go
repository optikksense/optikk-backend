package redis

import (
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type RedisHandler struct {
	modulecommon.DBTenant
	Service *RedisService
}

func (h *RedisHandler) withRange(c *gin.Context, fn func(teamID, startMs, endMs int64, instance string) error) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	instance := c.Query("instance")
	if err := fn(teamID, startMs, endMs, instance); err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
	}
}

func (h *RedisHandler) GetCacheHitRate(c *gin.Context) {
	h.withRange(c, func(teamID, startMs, endMs int64, instance string) error {
		resp, err := h.Service.GetCacheHitRate(teamID, startMs, endMs, instance)
		if err == nil {
			modulecommon.RespondOK(c, resp)
		}
		return err
	})
}

func (h *RedisHandler) GetReplicationLag(c *gin.Context) {
	h.withRange(c, func(teamID, startMs, endMs int64, instance string) error {
		resp, err := h.Service.GetReplicationLag(teamID, startMs, endMs, instance)
		if err == nil {
			modulecommon.RespondOK(c, resp)
		}
		return err
	})
}

func (h *RedisHandler) GetClients(c *gin.Context) {
	h.withRange(c, func(teamID, startMs, endMs int64, instance string) error {
		resp, err := h.Service.GetClients(teamID, startMs, endMs, instance)
		if err == nil {
			modulecommon.RespondOK(c, resp)
		}
		return err
	})
}

func (h *RedisHandler) GetMemory(c *gin.Context) {
	h.withRange(c, func(teamID, startMs, endMs int64, instance string) error {
		resp, err := h.Service.GetMemory(teamID, startMs, endMs, instance)
		if err == nil {
			modulecommon.RespondOK(c, resp)
		}
		return err
	})
}

func (h *RedisHandler) GetMemoryFragmentation(c *gin.Context) {
	h.withRange(c, func(teamID, startMs, endMs int64, instance string) error {
		resp, err := h.Service.GetMemoryFragmentation(teamID, startMs, endMs, instance)
		if err == nil {
			modulecommon.RespondOK(c, resp)
		}
		return err
	})
}

func (h *RedisHandler) GetCommands(c *gin.Context) {
	h.withRange(c, func(teamID, startMs, endMs int64, instance string) error {
		resp, err := h.Service.GetCommands(teamID, startMs, endMs, instance)
		if err == nil {
			modulecommon.RespondOK(c, resp)
		}
		return err
	})
}

func (h *RedisHandler) GetEvictions(c *gin.Context) {
	h.withRange(c, func(teamID, startMs, endMs int64, instance string) error {
		resp, err := h.Service.GetEvictions(teamID, startMs, endMs, instance)
		if err == nil {
			modulecommon.RespondOK(c, resp)
		}
		return err
	})
}

func (h *RedisHandler) GetKeyspace(c *gin.Context) {
	h.withRange(c, func(teamID, startMs, endMs int64, instance string) error {
		resp, err := h.Service.GetKeyspace(teamID, startMs, endMs, instance)
		if err == nil {
			modulecommon.RespondOK(c, resp)
		}
		return err
	})
}

func (h *RedisHandler) GetKeyExpiries(c *gin.Context) {
	h.withRange(c, func(teamID, startMs, endMs int64, instance string) error {
		resp, err := h.Service.GetKeyExpiries(teamID, startMs, endMs, instance)
		if err == nil {
			modulecommon.RespondOK(c, resp)
		}
		return err
	})
}
