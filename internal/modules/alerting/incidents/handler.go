package incidents

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler owns /alerts/incidents, /alerts/activity, /alerts/instances/:id/ack,
// /alerts/instances/:id/snooze.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func parseAlertID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid alert id", err)
		return 0, false
	}
	return id, true
}

func (h *Handler) List(c *gin.Context) {
	tenant := h.GetTenant(c)
	out, err := h.Service.List(c.Request.Context(), tenant.TeamID, c.Query("state"))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list incidents", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

func (h *Handler) ListActivity(c *gin.Context) {
	tenant := h.GetTenant(c)
	limit := modulecommon.ParseIntParam(c, "limit", 100)
	out, err := h.Service.ListActivity(c.Request.Context(), tenant.TeamID, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list activity", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

func (h *Handler) Ack(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	var req AckInstanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	if err := h.Service.Ack(c.Request.Context(), tenant.TeamID, tenant.UserID, id, req); err != nil {
		if errors.Is(err, shared.ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to ack instance", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"acked": true})
}

func (h *Handler) Snooze(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	var req SnoozeInstanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	if err := h.Service.Snooze(c.Request.Context(), tenant.TeamID, tenant.UserID, id, req); err != nil {
		if errors.Is(err, shared.ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to snooze instance", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"snoozed": true})
}
