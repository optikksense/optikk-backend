package rules

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler owns all /alerts/rules/* HTTP endpoints plus /rules/:id/mute/test/
// backtest/audit. Instance- and silence-level endpoints live in sibling
// submodules.
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

// Create POST /alerts/rules
func (h *Handler) Create(c *gin.Context) {
	tenant := h.GetTenant(c)
	var req CreateRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.Create(c.Request.Context(), tenant.TeamID, tenant.UserID, req)
	if err != nil {
		if errors.Is(err, shared.ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "failed to create rule", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// List GET /alerts/rules
func (h *Handler) List(c *gin.Context) {
	tenant := h.GetTenant(c)
	out, err := h.Service.List(c.Request.Context(), tenant.TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list rules", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// Preview POST /alerts/rules/preview
func (h *Handler) Preview(c *gin.Context) {
	var req CreateRuleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.Preview(c.Request.Context(), req)
	if err != nil {
		if errors.Is(err, shared.ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "failed to preview rule", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// Get GET /alerts/rules/:id
func (h *Handler) Get(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	rule, err := h.Service.Get(c.Request.Context(), tenant.TeamID, id)
	if err != nil {
		if errors.Is(err, shared.ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to load rule", err)
		return
	}
	modulecommon.RespondOK(c, rule)
}

// Update PATCH /alerts/rules/:id
func (h *Handler) Update(c *gin.Context) {
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
	out, err := h.Service.Update(c.Request.Context(), tenant.TeamID, tenant.UserID, id, req)
	if err != nil {
		if errors.Is(err, shared.ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		if errors.Is(err, shared.ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "failed to update rule", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// Delete DELETE /alerts/rules/:id
func (h *Handler) Delete(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	if err := h.Service.Delete(c.Request.Context(), tenant.TeamID, id); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to delete rule", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"deleted": true})
}

// Mute POST /alerts/rules/:id/mute
func (h *Handler) Mute(c *gin.Context) {
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
	if err := h.Service.Mute(c.Request.Context(), tenant.TeamID, tenant.UserID, id, req); err != nil {
		if errors.Is(err, shared.ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to mute rule", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"muted": true})
}

// Test POST /alerts/rules/:id/test
func (h *Handler) Test(c *gin.Context) {
	tenant := h.GetTenant(c)
	id, ok := parseAlertID(c)
	if !ok {
		return
	}
	out, err := h.Service.Test(c.Request.Context(), tenant.TeamID, id)
	if err != nil {
		if errors.Is(err, shared.ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		if errors.Is(err, shared.ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to test rule", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

// Backtest POST /alerts/rules/:id/backtest
func (h *Handler) Backtest(c *gin.Context) {
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
	out, err := h.Service.Backtest(c.Request.Context(), tenant.TeamID, id, req)
	if err != nil {
		if errors.Is(err, shared.ErrRuleNotFound) {
			modulecommon.RespondErrorWithCause(c, http.StatusNotFound, errorcode.AlertRuleNotFound, "rule not found", err)
			return
		}
		if errors.Is(err, shared.ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to backtest rule", err)
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
