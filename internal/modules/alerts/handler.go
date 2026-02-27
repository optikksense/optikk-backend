package alerts

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/alerts/service"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// AlertHandler handles alert and incident API endpoints.
type AlertHandler struct {
	modulecommon.DBTenant
	Service service.Service
}

// orgIDFromTeam looks up the organization_id for a given team.
func orgIDFromTeam(db *sql.DB, teamID int64) int64 {
	var orgID int64
	_ = db.QueryRow("SELECT organization_id FROM teams WHERE id = ? LIMIT 1", teamID).Scan(&orgID)
	if orgID == 0 {
		orgID = 1
	}
	return orgID
}

// GetAlerts — list alerts with optional status filter.
func (h *AlertHandler) GetAlerts(c *gin.Context) {
	tenant := h.GetTenant(c)
	status := c.Query("status")

	alerts, err := h.Service.GetAlerts(tenant.TeamID, status)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load alerts")
		return
	}
	RespondOK(c, alerts)
}

// GetAlertsPaged — paginated alert list.
func (h *AlertHandler) GetAlertsPaged(c *gin.Context) {
	tenant := h.GetTenant(c)
	page := ParseIntParam(c, "page", 0)
	size := ParseIntParam(c, "size", 20)

	pageResp, err := h.Service.GetAlertsPaged(tenant.TeamID, page, size)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load alerts")
		return
	}
	RespondOK(c, pageResp)
}

// GetAlertByID — single alert by primary key.
func (h *AlertHandler) GetAlertByID(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert id")
		return
	}
	alert, err := h.Service.GetAlertByID(id)
	if err != nil || alert == nil {
		RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Alert not found")
		return
	}
	RespondOK(c, alert)
}

// CreateAlert — create a new alert rule.
func (h *AlertHandler) CreateAlert(c *gin.Context) {
	var req types.AlertRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert payload")
		return
	}
	tenant := h.GetTenant(c)
	orgID := orgIDFromTeam(h.DB, tenant.TeamID)

	alert, err := h.Service.CreateAlert(orgID, tenant.TeamID, req)
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Failed to create alert")
		return
	}
	RespondOK(c, alert)
}

// AcknowledgeAlert — mark alert as acknowledged.
func (h *AlertHandler) AcknowledgeAlert(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert id")
		return
	}
	user := h.GetTenant(c).UserEmail

	alert, err := h.Service.AcknowledgeAlert(id, user)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update alert")
		return
	}
	RespondOK(c, alert)
}

// ResolveAlert — mark alert as resolved.
func (h *AlertHandler) ResolveAlert(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert id")
		return
	}
	user := h.GetTenant(c).UserEmail

	alert, err := h.Service.ResolveAlert(id, user)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update alert")
		return
	}
	RespondOK(c, alert)
}

// MuteAlert — mute alert for configurable duration.
func (h *AlertHandler) MuteAlert(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert id")
		return
	}
	user := h.GetTenant(c).UserEmail
	minutes := ParseIntParam(c, "minutes", 60)

	alert, err := h.Service.MuteAlert(id, minutes, user)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update alert")
		return
	}
	RespondOK(c, alert)
}

// MuteAlertWithReason — mute alert with a reason string.
func (h *AlertHandler) MuteAlertWithReason(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert id")
		return
	}
	user := h.GetTenant(c).UserEmail
	minutes := ParseIntParam(c, "minutes", 60)
	var body map[string]string
	_ = c.ShouldBindJSON(&body)
	reason := body["reason"]

	alert, err := h.Service.MuteAlertWithReason(id, minutes, reason, user)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to mute alert")
		return
	}
	RespondOK(c, alert)
}

// BulkMuteAlerts — mute multiple alerts at once.
func (h *AlertHandler) BulkMuteAlerts(c *gin.Context) {
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	ids := dbutil.ToInt64Slice(body["ids"])
	if len(ids) == 0 {
		RespondOK(c, []map[string]any{})
		return
	}
	minutes := int(dbutil.Int64FromAny(body["minutes"]))
	if minutes == 0 {
		minutes = 60
	}
	reason := dbutil.StringFromAny(body["reason"])

	alerts, err := h.Service.BulkMuteAlerts(ids, minutes, reason)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to mute alerts")
		return
	}
	RespondOK(c, alerts)
}

// BulkResolveAlerts — resolve multiple alerts at once.
func (h *AlertHandler) BulkResolveAlerts(c *gin.Context) {
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid request body")
		return
	}
	ids := dbutil.ToInt64Slice(body["ids"])
	if len(ids) == 0 {
		RespondOK(c, []map[string]any{})
		return
	}
	resolvedBy := h.GetTenant(c).UserEmail

	alerts, err := h.Service.BulkResolveAlerts(ids, resolvedBy)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to resolve alerts")
		return
	}
	RespondOK(c, alerts)
}

// GetAlertsForIncident — alerts linked to an incident policy.
func (h *AlertHandler) GetAlertsForIncident(c *gin.Context) {
	tenant := h.GetTenant(c)
	policyID := c.Param("policyId")

	alerts, err := h.Service.GetAlertsForIncident(tenant.TeamID, policyID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load alerts")
		return
	}
	RespondOK(c, alerts)
}

// CountActiveAlerts — count of active (non-resolved/muted) alerts.
func (h *AlertHandler) CountActiveAlerts(c *gin.Context) {
	tenant := h.GetTenant(c)
	count := h.Service.CountActiveAlerts(tenant.TeamID)
	RespondOK(c, count)
}

// GetIncidents — incidents view built from the alerts table.
func (h *AlertHandler) GetIncidents(c *gin.Context) {
	tenant := h.GetTenant(c)
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)
	statuses := ParseListParam(c, "statuses")
	severities := ParseListParam(c, "severities")
	services := ParseListParam(c, "services")
	limit := ParseIntParam(c, "limit", 100)
	offset := ParseIntParam(c, "offset", 0)

	incidents, err := h.Service.GetIncidents(tenant.TeamID, startMs, endMs, statuses, severities, services, limit, offset)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query incidents")
		return
	}
	RespondOK(c, incidents)
}
