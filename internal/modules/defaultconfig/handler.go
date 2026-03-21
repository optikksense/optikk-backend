package defaultconfig

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// Handler serves default-config APIs.
type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) ListPages(c *gin.Context) {
	tenant := h.GetTenant(c)
	pages := h.Service.ListPages(tenant.TeamID)
	RespondOK(c, map[string]any{"pages": pages})
}

func (h *Handler) ListTabs(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	if pageID == "" {
		RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "pageId is required")
		return
	}

	doc, err := h.Service.ListTabs(tenant.TeamID, pageID)
	if err != nil {
		RespondError(c, http.StatusNotFound, errorcode.NotFound, err.Error())
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

func (h *Handler) ListComponents(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	tabID := c.Param("tabId")
	if pageID == "" || tabID == "" {
		RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "pageId and tabId are required")
		return
	}

	doc, err := h.Service.ListComponents(tenant.TeamID, pageID)
	if err != nil {
		RespondError(c, http.StatusNotFound, errorcode.NotFound, err.Error())
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

	RespondError(c, http.StatusNotFound, errorcode.NotFound, "No tab found for page: "+pageID+" tab: "+tabID)
}

// SavePageOverride saves a page-level override JSON blob.
func (h *Handler) SavePageOverride(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	if pageID == "" {
		RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "pageId is required")
		return
	}

	var override configdefaults.PageDocument
	if err := c.ShouldBindJSON(&override); err != nil {
		RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "invalid JSON override")
		return
	}

	if err := h.Service.SavePageOverride(tenant.TeamID, pageID, override); err != nil {
		status := http.StatusInternalServerError
		code := errorcode.Internal
		if _, ok := err.(httpError); ok {
			status = http.StatusBadRequest
			code = errorcode.BadRequest
			if pageID != "" && override.Page.ID == "" {
				// Preserve not-found behavior for unknown pages.
				if err.Error() == "No configuration found for page: "+pageID {
					status = http.StatusNotFound
					code = errorcode.NotFound
				}
			}
		}
		RespondError(c, status, code, err.Error())
		return
	}

	RespondOK(c, map[string]any{
		"pageId":  pageID,
		"message": "Default config override saved successfully",
	})
}

type httpError string

func (e httpError) Error() string { return string(e) }
