package health

import (
	"net/http"

	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/health/service"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
)

// HealthHandler handles health-check CRUD and result query endpoints.
type HealthHandler struct {
	modulecommon.DBTenant
	Service service.Service
}

// GetHealthChecks — list all health checks for the tenant.
func (h *HealthHandler) GetHealthChecks(c *gin.Context) {
	tenant := h.GetTenant(c)
	rows, err := h.Service.GetHealthChecks(tenant.TeamID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load health checks")
		return
	}
	RespondOK(c, rows)
}

// CreateHealthCheck — create a new health check definition.
func (h *HealthHandler) CreateHealthCheck(c *gin.Context) {
	var req types.HealthCheckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid payload")
		return
	}
	tenant := h.GetTenant(c)

	row, err := h.Service.CreateHealthCheck(tenant.TeamID, tenant.OrganizationID, req)
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Failed to create health check")
		return
	}
	RespondOK(c, row)
}

// UpdateHealthCheck — update an existing health check.
func (h *HealthHandler) UpdateHealthCheck(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid health check id")
		return
	}
	var req types.HealthCheckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid payload")
		return
	}

	row, err := h.Service.UpdateHealthCheck(id, req)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update health check")
		return
	}
	RespondOK(c, row)
}

// DeleteHealthCheck — remove a health check definition.
func (h *HealthHandler) DeleteHealthCheck(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid health check id")
		return
	}
	err = h.Service.DeleteHealthCheck(id)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete health check")
		return
	}
	RespondOK(c, nil)
}

// ToggleHealthCheck — enable/disable a health check.
func (h *HealthHandler) ToggleHealthCheck(c *gin.Context) {
	id, err := ExtractIDParam(c, "id")
	if err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid health check id")
		return
	}

	row, err := h.Service.ToggleHealthCheck(id)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to toggle health check")
		return
	}
	RespondOK(c, row)
}

// GetHealthCheckStatus — aggregated uptime / response time summary per check.
func (h *HealthHandler) GetHealthCheckStatus(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)

	rows, err := h.Service.GetHealthCheckStatus(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query health check status")
		return
	}
	RespondOK(c, rows)
}

// GetHealthCheckResults — individual check results for a specific check.
func (h *HealthHandler) GetHealthCheckResults(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	checkID := c.Param("checkId")
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)
	limit := ParseIntParam(c, "limit", 100)
	offset := ParseIntParam(c, "offset", 0)

	rows, err := h.Service.GetHealthCheckResults(teamUUID, checkID, startMs, endMs, limit, offset)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query health check results")
		return
	}
	RespondOK(c, rows)
}

// GetHealthCheckTrend — aggregated response-time trend for a check.
func (h *HealthHandler) GetHealthCheckTrend(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	checkID := c.Param("checkId")
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)

	rows, err := h.Service.GetHealthCheckTrend(teamUUID, checkID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query health check trend")
		return
	}
	RespondOK(c, rows)
}
