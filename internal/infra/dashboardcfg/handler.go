package dashboardcfg

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler serves default-config APIs.
type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

type listPagesResponse struct {
	Pages []PageMetadata `json:"pages"`
}

type listTabsResponse struct {
	PageID string          `json:"pageId"`
	Tabs   []tabSummaryDTO `json:"tabs"`
}

type tabSummaryDTO struct {
	ID     string `json:"id"`
	PageID string `json:"pageId"`
	Label  string `json:"label"`
	Order  int    `json:"order"`
}

type getTabDocumentResponse struct {
	PageID   string              `json:"pageId"`
	TabID    string              `json:"tabId"`
	ID       string              `json:"id"`
	Label    string              `json:"label"`
	Order    int                 `json:"order"`
	Sections []SectionDefinition `json:"sections"`
	Panels   []PanelDefinition   `json:"panels"`
}

func (h *Handler) ListPages(c *gin.Context) {
	tenant := h.GetTenant(c)
	pages := h.Service.ListPages(tenant.TeamID)
	modulecommon.RespondOK(c, listPagesResponse{Pages: pages})
}

func (h *Handler) ListTabs(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	if pageID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "pageId is required")
		return
	}

	doc, err := h.Service.ListTabs(tenant.TeamID, pageID)
	if err != nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, err.Error())
		return
	}

	tabs := make([]tabSummaryDTO, 0, len(doc.Tabs))
	for _, tab := range doc.Tabs {
		tabs = append(tabs, tabSummaryDTO{
			ID:     tab.ID,
			PageID: tab.PageID,
			Label:  tab.Label,
			Order:  tab.Order,
		})
	}

	modulecommon.RespondOK(c, listTabsResponse{PageID: pageID, Tabs: tabs})
}

func (h *Handler) GetTabDocument(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	tabID := c.Param("tabId")
	if pageID == "" || tabID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "pageId and tabId are required")
		return
	}

	tab, err := h.Service.GetTabDocument(tenant.TeamID, pageID, tabID)
	if err != nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, err.Error())
		return
	}

	modulecommon.RespondOK(c, getTabDocumentResponse{
		PageID:   pageID,
		TabID:    tabID,
		ID:       tab.ID,
		Label:    tab.Label,
		Order:    tab.Order,
		Sections: tab.Sections,
		Panels:   tab.Panels,
	})
}
