package alerting

import (
	"errors"
	"net/http"
	"strconv"

	errorcode "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler serves the alerting HTTP surface.
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

// CreateRule POST /alerts/rules
func (h *Handler) CreateRule(c *gin.Context) {
	tenant := h.GetTenant(c)
	var req CreateRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.CreateRule(c.Request.Context(), tenant.TeamID, tenant.UserID, req)
	if err != nil {
		if errors.Is(err, ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "failed to create rule", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// ListRules GET /alerts/rules
func (h *Handler) ListRules(c *gin.Context) {
	tenant := h.GetTenant(c)
	rules, err := h.Service.ListRules(c.Request.Context(), tenant.TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list rules", err)
		return
	}
	modulecommon.RespondOK(c, rules)
}

// PreviewRule POST /alerts/rules/preview
func (h *Handler) PreviewRule(c *gin.Context) {
	var req CreateRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.PreviewRule(c.Request.Context(), req)
	if err != nil {
		if errors.Is(err, ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "failed to preview rule", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// GetRule GET /alerts/rules/:id
func (h *Handler) GetRule(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	rule, err := h.Service.GetRule(c.Request.Context(), tenant.TeamID, id)
	if err != nil {
		if errors.Is(err, ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to load rule", err)
		return
	}
	modulecommon.RespondOK(c, rule)
}

// UpdateRule PATCH /alerts/rules/:id
func (h *Handler) UpdateRule(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	var req UpdateRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.UpdateRule(c.Request.Context(), tenant.TeamID, tenant.UserID, id, req)
	if err != nil {
		if errors.Is(err, ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		if errors.Is(err, ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "failed to update rule", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// DeleteRule DELETE /alerts/rules/:id
func (h *Handler) DeleteRule(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	if err := h.Service.DeleteRule(c.Request.Context(), tenant.TeamID, id); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to delete rule", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"deleted": true})
}

// ListIncidents GET /alerts/incidents
func (h *Handler) ListIncidents(c *gin.Context) {
	tenant := h.GetTenant(c)
	out, err := h.Service.ListIncidents(c.Request.Context(), tenant.TeamID, c.Query("state"))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list incidents", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// ListActivity GET /alerts/activity
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

// MuteRule POST /alerts/rules/:id/mute
func (h *Handler) MuteRule(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	var req MuteRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	if err := h.Service.MuteRule(c.Request.Context(), tenant.TeamID, tenant.UserID, id, req); err != nil {
		if errors.Is(err, ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to mute rule", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"muted": true})
}

// TestRule POST /alerts/rules/:id/test
func (h *Handler) TestRule(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	out, err := h.Service.TestRule(c.Request.Context(), tenant.TeamID, id)
	if err != nil {
		if errors.Is(err, ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		if errors.Is(err, ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to test rule", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// BacktestRule POST /alerts/rules/:id/backtest
func (h *Handler) BacktestRule(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	var req BacktestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.BacktestRule(c.Request.Context(), tenant.TeamID, id, req)
	if err != nil {
		if errors.Is(err, ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		if errors.Is(err, ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to backtest rule", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// AckInstance POST /alerts/instances/:id/ack
func (h *Handler) AckInstance(c *gin.Context) {
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
	if err := h.Service.AckInstance(c.Request.Context(), tenant.TeamID, tenant.UserID, id, req); err != nil {
		if errors.Is(err, ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to ack instance", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"acked": true})
}

// SnoozeInstance POST /alerts/instances/:id/snooze
func (h *Handler) SnoozeInstance(c *gin.Context) {
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
	if err := h.Service.SnoozeInstance(c.Request.Context(), tenant.TeamID, tenant.UserID, id, req); err != nil {
		if errors.Is(err, ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to snooze instance", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"snoozed": true})
}

// ListSilences GET /alerts/silences
func (h *Handler) ListSilences(c *gin.Context) {
	tenant := h.GetTenant(c)
	out, err := h.Service.ListSilences(c.Request.Context(), tenant.TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list silences", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// CreateSilence POST /alerts/silences
func (h *Handler) CreateSilence(c *gin.Context) {
	tenant := h.GetTenant(c)
	var req CreateSilenceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.CreateSilence(c.Request.Context(), tenant.TeamID, tenant.UserID, req)
	if err != nil {
		if errors.Is(err, ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to create silence", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// UpdateSilence PATCH /alerts/silences/:id
func (h *Handler) UpdateSilence(c *gin.Context) {
	tenant := h.GetTenant(c)
	silenceID := c.Param("id")
	alertID, err := strconv.ParseInt(c.Query("alertId"), 10, 64)
	if err != nil || alertID <= 0 {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "alertId query param required", err)
		return
	}
	var req UpdateSilenceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.UpdateSilence(c.Request.Context(), tenant.TeamID, alertID, silenceID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to update silence", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// DeleteSilence DELETE /alerts/silences/:id
func (h *Handler) DeleteSilence(c *gin.Context) {
	tenant := h.GetTenant(c)
	silenceID := c.Param("id")
	alertID, err := strconv.ParseInt(c.Query("alertId"), 10, 64)
	if err != nil || alertID <= 0 {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "alertId query param required", err)
		return
	}
	if err := h.Service.DeleteSilence(c.Request.Context(), tenant.TeamID, alertID, silenceID); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to delete silence", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"deleted": true})
}

// SlackCallback POST /alerts/callback/slack
func (h *Handler) SlackCallback(c *gin.Context) {
	var payload map[string]any
	if err := c.ShouldBindJSON(&payload); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	if err := h.Service.HandleSlackCallback(c.Request.Context(), payload); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to handle slack callback", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"ok": true})
}

// TestSlack POST /alerts/slack/test
func (h *Handler) TestSlack(c *gin.Context) {
	var req SlackTestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.TestSlack(c.Request.Context(), req)
	if err != nil {
		if errors.Is(err, ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "failed to test slack webhook", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// ListAudit GET /alerts/rules/:id/audit
func (h *Handler) ListAudit(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 200)
	out, err := h.Service.ListAudit(c.Request.Context(), tenant.TeamID, id, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list audit", err)
		return
	}
	modulecommon.RespondOK(c, out)
}
