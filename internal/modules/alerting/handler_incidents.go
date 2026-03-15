package alerting

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
)

func (h *Handler) ListIncidents(c *gin.Context) {
	tenant := h.getTenant(c)
	status := c.Query("status")
	limit := common.ParseIntParam(c, "limit", 50)

	incidents, err := h.svc.ListIncidents(tenant.TeamID, status, limit)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list incidents")
		return
	}
	common.RespondOK(c, incidents)
}

func (h *Handler) GetIncidentByID(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid incident ID")
		return
	}
	incident, err := h.svc.GetIncidentByID(tenant.TeamID, id)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get incident")
		return
	}
	if incident == nil {
		common.RespondError(c, http.StatusNotFound, "NOT_FOUND", "Incident not found")
		return
	}
	common.RespondOK(c, incident)
}

func (h *Handler) UpdateIncidentStatus(c *gin.Context) {
	tenant := h.getTenant(c)
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid incident ID")
		return
	}
	var action IncidentAction
	if err := c.ShouldBindJSON(&action); err != nil {
		common.RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request: "+err.Error())
		return
	}
	incident, err := h.svc.UpdateIncidentStatus(tenant.TeamID, tenant.UserID, id, action)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, incident)
}

func (h *Handler) GetAlertSummary(c *gin.Context) {
	tenant := h.getTenant(c)
	summary, err := h.svc.GetAlertSummary(tenant.TeamID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get alert summary")
		return
	}
	common.RespondOK(c, summary)
}
