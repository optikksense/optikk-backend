package alerts

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// AlertHandler handles alert and incident API endpoints.
type AlertHandler struct {
	modulecommon.DBTenant
	AlertCondCol string // resolved once at startup
	Repo         *Repository
}

// GetAlerts — list alerts with optional status filter.
func (h *AlertHandler) GetAlerts(c *gin.Context) {
	tenant := h.GetTenant(c)
	status := c.Query("status")
	
	rows, err := h.Repo.GetAlerts(tenant.TeamID, status)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load alerts")
		return
	}
	for _, row := range rows {
		normalizeAlertRow(row)
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetAlertsPaged — paginated alert list.
func (h *AlertHandler) GetAlertsPaged(c *gin.Context) {
	tenant := h.GetTenant(c)
	page := ParseIntParam(c, "page", 0)
	size := ParseIntParam(c, "size", 20)
	if size <= 0 {
		size = 20
	}
	offset := page * size

	rows, total, err := h.Repo.GetAlertsPaged(tenant.TeamID, size, offset)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load alerts")
		return
	}
	for _, row := range rows {
		normalizeAlertRow(row)
	}

	totalPages := int((total + int64(size) - 1) / int64(size))
	RespondOK(c, map[string]any{
		"content":          NormalizeRows(rows),
		"pageable":         map[string]any{"pageNumber": page, "pageSize": size, "offset": offset},
		"totalElements":    total,
		"totalPages":       totalPages,
		"size":             size,
		"number":           page,
		"first":            page == 0,
		"last":             page+1 >= totalPages,
		"numberOfElements": len(rows),
	})
}

// GetAlertByID — single alert by primary key.
func (h *AlertHandler) GetAlertByID(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert id")
		return
	}
	row, err := h.Repo.GetAlertByID(id)
	if err != nil || len(row) == 0 {
		RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Alert not found")
		return
	}
	normalizeAlertRow(row)
	RespondOK(c, row)
}

// CreateAlert — create a new alert rule.
func (h *AlertHandler) CreateAlert(c *gin.Context) {
	var req types.AlertRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert payload")
		return
	}
	tenant := h.GetTenant(c)

	id, err := h.Repo.CreateAlert(tenant.OrganizationID, tenant.TeamID, req)
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Failed to create alert")
		return
	}
	c.Params = append(c.Params, gin.Param{Key: "id", Value: strconv.FormatInt(id, 10)})
	h.GetAlertByID(c)
}

// AcknowledgeAlert — mark alert as acknowledged.
func (h *AlertHandler) AcknowledgeAlert(c *gin.Context) {
	h.updateAlertState(c, "acknowledged", true, false, false)
}

// ResolveAlert — mark alert as resolved.
func (h *AlertHandler) ResolveAlert(c *gin.Context) {
	h.updateAlertState(c, "resolved", false, true, false)
}

// MuteAlert — mute alert for configurable duration.
func (h *AlertHandler) MuteAlert(c *gin.Context) {
	h.updateAlertState(c, "muted", false, false, true)
}

// MuteAlertWithReason — mute alert with a reason string.
func (h *AlertHandler) MuteAlertWithReason(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert id")
		return
	}
	minutes := ParseIntParam(c, "minutes", 60)
	var body map[string]string
	_ = c.ShouldBindJSON(&body)
	reason := body["reason"]
	
	err = h.Repo.MuteAlertWithReason(id, minutes, reason)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to mute alert")
		return
	}
	c.Params = append(c.Params, gin.Param{Key: "id", Value: strconv.FormatInt(id, 10)})
	h.GetAlertByID(c)
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

	rows, err := h.Repo.BulkMuteAlerts(ids, minutes, reason)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to mute alerts")
		return
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		normalizeAlertRow(row)
		out = append(out, row)
	}
	RespondOK(c, out)
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

	rows, err := h.Repo.BulkResolveAlerts(ids, resolvedBy)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to resolve alerts")
		return
	}
	
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		normalizeAlertRow(row)
		out = append(out, row)
	}
	RespondOK(c, out)
}

// GetAlertsForIncident — alerts linked to an incident policy.
func (h *AlertHandler) GetAlertsForIncident(c *gin.Context) {
	tenant := h.GetTenant(c)
	policyID := c.Param("policyId")
	
	rows, err := h.Repo.GetAlertsForIncident(tenant.TeamID, policyID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load alerts")
		return
	}
	for _, row := range rows {
		normalizeAlertRow(row)
	}
	RespondOK(c, NormalizeRows(rows))
}

// CountActiveAlerts — count of active (non-resolved/muted) alerts.
func (h *AlertHandler) CountActiveAlerts(c *gin.Context) {
	tenant := h.GetTenant(c)
	count := h.Repo.CountActiveAlerts(tenant.TeamID)
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

	alertStatuses := alertStatusesFromIncidentStatuses(statuses)
	
	items, total, rawStatusCounts, severityCounts, err := h.Repo.GetIncidents(tenant.TeamID, startMs, endMs, alertStatuses, severities, services, limit, offset)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query incidents")
		return
	}

	statusCounts := map[string]int64{}
	for st, count := range rawStatusCounts {
		statusCounts[incidentStatusFromAlertStatus(st)] += count
	}

	incidents := make([]map[string]any, 0, len(items))
	for _, alert := range items {
		incidentID := dbutil.Int64FromAny(alert["id"])
		policyID := dbutil.StringFromAny(alert["triggered_by"])
		createdAt := alert["created_at"]
		if alert["triggered_at"] != nil {
			createdAt = alert["triggered_at"]
		}
		incidents = append(incidents, map[string]any{
			"incident_id":     incidentID,
			"alert_policy_id": policyID,
			"triggered_by":    policyID,
			"title":           dbutil.StringFromAny(alert["name"]),
			"description":     dbutil.StringFromAny(alert["description"]),
			"severity":        dbutil.StringFromAny(alert["severity"]),
			"priority":        nil,
			"status":          incidentStatusFromAlertStatus(dbutil.StringFromAny(alert["status"])),
			"source":          dbutil.StringFromAny(alert["type"]),
			"service_name":    dbutil.StringFromAny(alert["service_name"]),
			"created_at":      createdAt,
			"updated_at":      alert["updated_at"],
			"resolved_at":     alert["resolved_at"],
			"acknowledged_at": alert["acknowledged_at"],
			"acknowledged_by": alert["acknowledged_by"],
			"pod":             nil,
			"container":       nil,
		})
	}

	RespondOK(c, map[string]any{
		"incidents": incidents,
		"hasMore":   len(incidents) >= limit,
		"offset":    offset,
		"limit":     limit,
		"total":     total,
		"counts": map[string]any{
			"byStatus":   statusCounts,
			"bySeverity": severityCounts,
		},
	})
}

// updateAlertState applies a state transition to a single alert.
func (h *AlertHandler) updateAlertState(c *gin.Context, status string, acknowledge, resolve, mute bool) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid alert id")
		return
	}
	user := h.GetTenant(c).UserEmail
	minutes := ParseIntParam(c, "minutes", 60)

	if err := h.Repo.UpdateAlertState(id, status, acknowledge, resolve, mute, user, minutes); err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update alert")
		return
	}
	c.Params = append(c.Params, gin.Param{Key: "id", Value: strconv.FormatInt(id, 10)})
	h.GetAlertByID(c)
}

// normalizeAlertRow converts DB column names to camelCase response fields.
func normalizeAlertRow(row map[string]any) {
	if v, ok := row["condition_expr"]; ok {
		row["condition"] = v
		delete(row, "condition_expr")
	}
	copyField(row, "organization_id", "organizationId")
	copyField(row, "team_id", "teamId")
	copyField(row, "service_name", "serviceName")
	copyField(row, "duration_minutes", "durationMinutes")
	copyField(row, "current_value", "currentValue")
	copyField(row, "triggered_at", "triggeredAt")
	copyField(row, "acknowledged_at", "acknowledgedAt")
	copyField(row, "acknowledged_by", "acknowledgedBy")
	copyField(row, "resolved_at", "resolvedAt")
	copyField(row, "resolved_by", "resolvedBy")
	copyField(row, "muted_until", "mutedUntil")
	copyField(row, "mute_reason", "muteReason")
	copyField(row, "runbook_url", "runbookUrl")
	copyField(row, "created_at", "createdAt")
	copyField(row, "updated_at", "updatedAt")
	if row["triggeredAt"] == nil {
		copyField(row, "created_at", "triggeredAt")
	}
}

func copyField(row map[string]any, src, dst string) {
	if v, ok := row[src]; ok {
		row[dst] = v
	}
}

func incidentStatusFromAlertStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "active":
		return "open"
	case "acknowledged":
		return "investigating"
	case "muted":
		return "monitoring"
	case "resolved":
		return "resolved"
	default:
		return strings.ToLower(strings.TrimSpace(status))
	}
}

func alertStatusesFromIncidentStatuses(statuses []string) []string {
	if len(statuses) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	for _, raw := range statuses {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "open":
			set["active"] = struct{}{}
		case "investigating", "identified":
			set["acknowledged"] = struct{}{}
		case "monitoring":
			set["muted"] = struct{}{}
		case "resolved":
			set["resolved"] = struct{}{}
		case "active", "acknowledged", "muted":
			set[strings.ToLower(strings.TrimSpace(raw))] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	return out
}

