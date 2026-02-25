package dashboardconfig

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// DashboardConfigHandler handles dashboard chart configuration endpoints.
type DashboardConfigHandler struct {
	modulecommon.DBTenant
	Repo        *Repository
	VersionRepo *VersionRepository
	ShareRepo   *ShareRepository
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
	yaml, err := h.Repo.GetConfig(tenant.TeamID, pageID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load dashboard config")
		return
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
		ConfigYaml    string `json:"configYaml"`
		ChangeSummary string `json:"changeSummary"`
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

	if err := h.Repo.SaveConfig(tenant.TeamID, pageID, body.ConfigYaml); err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save dashboard config")
		return
	}

	// Create a version snapshot if VersionRepo is available
	if h.VersionRepo != nil {
		createdBy := ""
		if tenant.UserEmail != "" {
			createdBy = tenant.UserEmail
		}
		if _, err := h.VersionRepo.SaveVersion(tenant.TeamID, pageID, body.ConfigYaml, body.ChangeSummary, createdBy); err != nil {
			// Log but don't fail the save
			c.Error(err)
		}
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

// ─── Versioning Endpoints ────────────────────────────────────────────────────

// ListConfigVersions returns version history for a page.
func (h *DashboardConfigHandler) ListConfigVersions(c *gin.Context) {
	if h.VersionRepo == nil {
		RespondError(c, http.StatusNotImplemented, "NOT_IMPLEMENTED", "Versioning not available")
		return
	}

	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	versions, err := h.VersionRepo.ListVersions(tenant.TeamID, pageID, limit, offset)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load version history")
		return
	}

	if versions == nil {
		versions = []VersionMeta{}
	}

	RespondOK(c, map[string]any{
		"pageId":   pageID,
		"versions": versions,
	})
}

// GetConfigVersion returns a specific version's YAML.
func (h *DashboardConfigHandler) GetConfigVersion(c *gin.Context) {
	if h.VersionRepo == nil {
		RespondError(c, http.StatusNotImplemented, "NOT_IMPLEMENTED", "Versioning not available")
		return
	}

	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	version, err := strconv.Atoi(c.Param("version"))
	if err != nil {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid version number")
		return
	}

	yaml, err := h.VersionRepo.GetVersion(tenant.TeamID, pageID, version)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load version")
		return
	}
	if yaml == "" {
		RespondError(c, http.StatusNotFound, "NOT_FOUND", "Version not found")
		return
	}

	RespondOK(c, map[string]any{
		"pageId":     pageID,
		"version":    version,
		"configYaml": yaml,
	})
}

// RollbackConfig restores a previous version as the current config.
func (h *DashboardConfigHandler) RollbackConfig(c *gin.Context) {
	if h.VersionRepo == nil {
		RespondError(c, http.StatusNotImplemented, "NOT_IMPLEMENTED", "Versioning not available")
		return
	}

	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")

	var body struct {
		Version int `json:"version"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Version <= 0 {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Valid version number is required")
		return
	}

	// Get the version's YAML
	yaml, err := h.VersionRepo.GetVersion(tenant.TeamID, pageID, body.Version)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to load version")
		return
	}
	if yaml == "" {
		RespondError(c, http.StatusNotFound, "NOT_FOUND", "Version not found")
		return
	}

	// Save as current config
	if err := h.Repo.SaveConfig(tenant.TeamID, pageID, yaml); err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to rollback config")
		return
	}

	// Create a new version entry for the rollback
	createdBy := ""
	if tenant.UserEmail != "" {
		createdBy = tenant.UserEmail
	}
	summary := "Rolled back to version " + strconv.Itoa(body.Version)
	if _, err := h.VersionRepo.SaveVersion(tenant.TeamID, pageID, yaml, summary, createdBy); err != nil {
		c.Error(err)
	}

	RespondOK(c, map[string]any{
		"pageId":  pageID,
		"message": "Rolled back to version " + strconv.Itoa(body.Version),
	})
}

// ─── Sharing Endpoints ───────────────────────────────────────────────────────

// CreateShare generates a shareable link for a dashboard.
func (h *DashboardConfigHandler) CreateShare(c *gin.Context) {
	if h.ShareRepo == nil {
		RespondError(c, http.StatusNotImplemented, "NOT_IMPLEMENTED", "Sharing not available")
		return
	}

	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")

	var body struct {
		Params         json.RawMessage `json:"params"`
		ExpiresInHours int             `json:"expiresInHours"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "Invalid request body")
		return
	}

	paramsJSON := "{}"
	if len(body.Params) > 0 {
		paramsJSON = string(body.Params)
	}

	shareID := GenerateShareID()

	var expiresAt *time.Time
	if body.ExpiresInHours > 0 {
		t := time.Now().Add(time.Duration(body.ExpiresInHours) * time.Hour)
		expiresAt = &t
	}

	createdBy := ""
	if tenant.UserEmail != "" {
		createdBy = tenant.UserEmail
	}

	if err := h.ShareRepo.CreateShare(tenant.TeamID, pageID, shareID, paramsJSON, createdBy, expiresAt); err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create share link")
		return
	}

	RespondOK(c, map[string]any{
		"shareId":  shareID,
		"shareUrl": "/shared/" + shareID,
	})
}

// GetShare resolves a share link to its parameters.
func (h *DashboardConfigHandler) GetShare(c *gin.Context) {
	if h.ShareRepo == nil {
		RespondError(c, http.StatusNotImplemented, "NOT_IMPLEMENTED", "Sharing not available")
		return
	}

	shareID := c.Param("shareId")
	if shareID == "" {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "shareId is required")
		return
	}

	record, err := h.ShareRepo.GetShare(shareID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to resolve share link")
		return
	}
	if record == nil {
		RespondError(c, http.StatusNotFound, "NOT_FOUND", "Share link not found or expired")
		return
	}

	// Parse params JSON back to object
	var params any
	if err := json.Unmarshal([]byte(record.ParamsJSON), &params); err != nil {
		params = json.RawMessage(record.ParamsJSON)
	}

	RespondOK(c, map[string]any{
		"shareId":   record.ShareID,
		"pageId":    record.PageID,
		"params":    params,
		"createdBy": record.CreatedBy,
		"createdAt": record.CreatedAt,
	})
}
