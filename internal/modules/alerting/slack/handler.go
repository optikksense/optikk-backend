package slack

import (
	"errors"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) TestSlack(c *gin.Context) {
	var req SlackTestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.TestSlack(c.Request.Context(), req)
	if err != nil {
		if errors.Is(err, shared.ErrUnsupportedCondition) {
			modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.AlertRuleInvalidCondition, "unsupported condition type", err)
			return
		}
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "failed to test slack webhook", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

func (h *Handler) Callback(c *gin.Context) {
	var payload map[string]any
	if err := c.ShouldBindJSON(&payload); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	if err := h.Service.HandleCallback(c.Request.Context(), payload); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to handle slack callback", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"ok": true})
}
