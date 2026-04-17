package silences

import (
	"net/http"
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) List(c *gin.Context) {
	tenant := h.GetTenant(c)
	out, err := h.Service.List(c.Request.Context(), tenant.TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list silences", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

func (h *Handler) Create(c *gin.Context) {
	tenant := h.GetTenant(c)
	var req CreateSilenceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "invalid body", err)
		return
	}
	out, err := h.Service.Create(c.Request.Context(), tenant.TeamID, tenant.UserID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to create silence", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

func (h *Handler) Update(c *gin.Context) {
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
	out, err := h.Service.Update(c.Request.Context(), tenant.TeamID, alertID, silenceID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to update silence", err)
		return
	}
	modulecommon.RespondOK(c, out)
}

func (h *Handler) Delete(c *gin.Context) {
	tenant := h.GetTenant(c)
	silenceID := c.Param("id")
	alertID, err := strconv.ParseInt(c.Query("alertId"), 10, 64)
	if err != nil || alertID <= 0 {
		modulecommon.RespondErrorWithCause(c, http.StatusBadRequest, errorcode.Validation, "alertId query param required", err)
		return
	}
	if err := h.Service.Delete(c.Request.Context(), tenant.TeamID, alertID, silenceID); err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to delete silence", err)
		return
	}
	modulecommon.RespondOK(c, gin.H{"deleted": true})
}
