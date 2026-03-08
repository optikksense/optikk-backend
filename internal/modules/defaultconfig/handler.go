package defaultconfig

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// Handler serves default-config APIs.
type Handler struct {
	modulecommon.DBTenant
	Repo     Repository
	Registry *configdefaults.Registry
}

// ListPages returns the ordered, navigable pages.
func (h *Handler) ListPages(c *gin.Context) {
	tenant := h.GetTenant(c)
	defaultPages := h.Registry.ListPages(true)
	pages := make([]configdefaults.PageMetadata, 0, len(defaultPages))
	for _, page := range defaultPages {
		doc, err := h.resolvePage(tenant.TeamID, page.ID)
		if err != nil {
			log.Printf("default-config: failed to resolve page %s for team %d: %v", page.ID, tenant.TeamID, err)
			continue
		}
		if doc.Page.Navigable {
			pages = append(pages, doc.Page)
		}
	}
	RespondOK(c, map[string]any{"pages": pages})
}

// ListTabs returns ordered tabs for a page.
func (h *Handler) ListTabs(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	if pageID == "" {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "pageId is required")
		return
	}

	doc, err := h.resolvePage(tenant.TeamID, pageID)
	if err != nil {
		RespondError(c, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}

	tabs := make([]map[string]any, 0, len(doc.Tabs))
	for _, tab := range doc.Tabs {
		tabs = append(tabs, map[string]any{
			"id":     tab.ID,
			"pageId": tab.PageID,
			"label":  tab.Label,
			"order":  tab.Order,
		})
	}

	RespondOK(c, map[string]any{
		"pageId": pageID,
		"tabs":   tabs,
	})
}

// ListComponents returns ordered renderable components for a page tab.
func (h *Handler) ListComponents(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	tabID := c.Param("tabId")
	if pageID == "" || tabID == "" {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "pageId and tabId are required")
		return
	}

	doc, err := h.resolvePage(tenant.TeamID, pageID)
	if err != nil {
		RespondError(c, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}

	for _, tab := range doc.Tabs {
		if tab.ID == tabID {
			RespondOK(c, map[string]any{
				"pageId":     pageID,
				"tabId":      tabID,
				"groups":     tab.Groups,
				"components": tab.Components,
			})
			return
		}
	}

	RespondError(c, http.StatusNotFound, "NOT_FOUND", "No tab found for page: "+pageID+" tab: "+tabID)
}

// SavePageOverride saves a page-level override JSON blob.
func (h *Handler) SavePageOverride(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	if pageID == "" {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "pageId is required")
		return
	}

	defaultDoc, ok := h.Registry.GetPage(pageID)
	if !ok {
		RespondError(c, http.StatusNotFound, "NOT_FOUND", "No configuration found for page: "+pageID)
		return
	}

	var override configdefaults.PageDocument
	if err := c.ShouldBindJSON(&override); err != nil {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON override")
		return
	}

	if override.Page.ID != "" && override.Page.ID != pageID {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", "page.id must match the route pageId")
		return
	}
	if override.Page.ID == "" {
		override.Page.ID = pageID
	}
	for i := range override.Tabs {
		if override.Tabs[i].PageID == "" {
			override.Tabs[i].PageID = pageID
		}
	}

	if _, err := configdefaults.MergePageDocument(defaultDoc, override); err != nil {
		RespondError(c, http.StatusBadRequest, "BAD_REQUEST", err.Error())
		return
	}

	bytes, err := json.Marshal(override)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to serialize override")
		return
	}
	if err := h.Repo.SavePageOverride(tenant.TeamID, pageID, string(bytes)); err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to save page override")
		return
	}

	RespondOK(c, map[string]any{
		"pageId":  pageID,
		"message": "Default config override saved successfully",
	})
}

func (h *Handler) resolvePage(teamID int64, pageID string) (configdefaults.PageDocument, error) {
	defaultDoc, ok := h.Registry.GetPage(pageID)
	if !ok {
		return configdefaults.PageDocument{}, httpError("No configuration found for page: " + pageID)
	}

	overrideJSON, err := h.Repo.GetPageOverride(teamID, pageID)
	if err != nil || overrideJSON == "" {
		return defaultDoc, nil
	}

	var override configdefaults.PageDocument
	if err := json.Unmarshal([]byte(overrideJSON), &override); err != nil {
		log.Printf("default-config: ignoring invalid override for page=%s team=%d: %v", pageID, teamID, err)
		return defaultDoc, nil
	}

	merged, err := configdefaults.MergePageDocument(defaultDoc, override)
	if err != nil {
		log.Printf("default-config: ignoring invalid merged override for page=%s team=%d: %v", pageID, teamID, err)
		return defaultDoc, nil
	}
	return merged, nil
}

type httpError string

func (e httpError) Error() string { return string(e) }
