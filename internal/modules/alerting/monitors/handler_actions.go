package monitors

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	httputil "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
)

func (h *Handler) Ack(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	if err := h.Service.Ack(c.Request.Context(), tenant.TeamID, tenant.UserID, id); err != nil {
		if errors.Is(err, ErrNotAlerting) {
			httputil.RespondError(c, http.StatusConflict, errorcode.Conflict, "monitor is not currently alerting")
			return
		}
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, gin.H{"acked": id})
}

func (h *Handler) Mute(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req MuteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.RespondError(c, http.StatusBadRequest, errorcode.Validation, err.Error())
		return
	}
	if err := h.Service.Mute(c.Request.Context(), tenant.TeamID, id, req.DurationSec); err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, gin.H{"muted": id, "duration_sec": req.DurationSec})
}

func (h *Handler) Unmute(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	if err := h.Service.Unmute(c.Request.Context(), tenant.TeamID, id); err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, gin.H{"unmuted": id})
}

func (h *Handler) Test(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	res, err := h.Service.Test(c.Request.Context(), tenant.TeamID, id, h.Queries)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) Series(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	windowMs := int64(httputil.ParseIntParam(c, "window_ms", 3_600_000))
	res, err := h.Service.Series(c.Request.Context(), tenant.TeamID, id, h.Queries, windowMs)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) Events(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	limit := httputil.ParseIntParam(c, "limit", 20)
	res, err := h.Service.Events(c.Request.Context(), tenant.TeamID, id, limit)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) Activity(c *gin.Context) {
	tenant := h.GetTenant(c)
	since := httputil.ParseInt64Param(c, "since", 0)
	limit := httputil.ParseIntParam(c, "limit", 20)
	res, err := h.Service.Activity(c.Request.Context(), tenant.TeamID, since, limit)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}

func (h *Handler) StatusTimeline(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseIDParam(c)
	if !ok {
		return
	}
	windowMs := int64(httputil.ParseIntParam(c, "window_ms", 24*60*60*1000))
	res, err := h.Service.StatusTimeline(c.Request.Context(), tenant.TeamID, id, windowMs)
	if err != nil {
		respondServiceError(c, err)
		return
	}
	httputil.RespondOK(c, res)
}
