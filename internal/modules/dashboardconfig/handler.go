package dashboardconfig

import (
	"io"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// DashboardConfigHandler handles dashboard chart configuration endpoints.
type DashboardConfigHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetDashboardConfig returns the chart configuration YAML for a page.
// If no team-specific config exists, returns the default config.
func (h *DashboardConfigHandler) GetDashboardConfig(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")

	if pageID == "" {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "pageId is required")
		return
	}

	// Try team-specific config first
	yaml, err := h.Service.GetConfig(tenant.TeamID, pageID)
	if err != nil {
		log.Printf("dashboard-config: team override lookup failed for page=%s team=%d: %v; falling back to default", pageID, tenant.TeamID, err)
	}

	// Fall back to default if no team-specific config
	if yaml == "" {
		defaultYaml, exists := GetDefaultConfig(pageID)
		if !exists {
			RespondError(c, http.StatusNotFound, "NOT_FOUND", "No configuration found for page: "+pageID)
			return
		}
		yaml = defaultYaml
	}

	RespondOK(c, map[string]any{
		"pageId":     pageID,
		"configYaml": yaml,
	})
}

// SaveDashboardConfig saves or updates the chart configuration YAML for a page.
func (h *DashboardConfigHandler) SaveDashboardConfig(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")

	if pageID == "" {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "pageId is required")
		return
	}

	var body struct {
		ConfigYaml string `json:"configYaml"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		// Try reading raw body as YAML
		raw, readErr := io.ReadAll(c.Request.Body)
		if readErr != nil || len(raw) == 0 {
			RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "configYaml is required")
			return
		}
		body.ConfigYaml = string(raw)
	}

	if body.ConfigYaml == "" {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "configYaml cannot be empty")
		return
	}

	if err := h.Service.SaveConfig(tenant.TeamID, pageID, body.ConfigYaml); err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save dashboard config")
		return
	}

	RespondOK(c, map[string]any{
		"pageId":  pageID,
		"message": "Dashboard config saved successfully",
	})
}

// ListPages returns the list of available page IDs with their default configs.
func (h *DashboardConfigHandler) ListPages(c *gin.Context) {
	configs := GetAllDefaultConfigs()
	pages := make([]string, 0, len(configs))
	for pageID := range configs {
		pages = append(pages, pageID)
	}
	RespondOK(c, map[string]any{
		"pages": pages,
	})
}
