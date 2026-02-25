package health

import (
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
	"net/http"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
)

// HealthHandler handles health-check CRUD and result query endpoints.
type HealthHandler struct {
	modulecommon.DBTenant
	Repo *Repository
}

// GetHealthChecks — list all health checks for the tenant.
func (h *HealthHandler) GetHealthChecks(c *gin.Context) {
	tenant := h.GetTenant(c)
	rows, err := h.Repo.GetHealthChecks(tenant.TeamID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load health checks")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// CreateHealthCheck — create a new health check definition.
func (h *HealthHandler) CreateHealthCheck(c *gin.Context) {
	var req types.HealthCheckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid payload")
		return
	}
	tenant := h.GetTenant(c)
	
	row, err := h.Repo.CreateHealthCheck(tenant.TeamID, tenant.OrganizationID, req)
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
	
	row, err := h.Repo.UpdateHealthCheck(id, req)
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
	err = h.Repo.DeleteHealthCheck(id)
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
	
	row, err := h.Repo.ToggleHealthCheck(id)
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
	
	rows, err := h.Repo.GetHealthCheckStatus(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query health check status")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetHealthCheckResults — individual check results for a specific check.
func (h *HealthHandler) GetHealthCheckResults(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	checkID := c.Param("checkId")
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)
	limit := ParseIntParam(c, "limit", 100)
	offset := ParseIntParam(c, "offset", 0)
	
	rows, err := h.Repo.GetHealthCheckResults(teamUUID, checkID, startMs, endMs, limit, offset)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query health check results")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetHealthCheckTrend — aggregated response-time trend for a check.
func (h *HealthHandler) GetHealthCheckTrend(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	checkID := c.Param("checkId")
	startMs, endMs := ParseRange(c, 7*24*60*60*1000)
	
	rows, err := h.Repo.GetHealthCheckTrend(teamUUID, checkID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query health check trend")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}
