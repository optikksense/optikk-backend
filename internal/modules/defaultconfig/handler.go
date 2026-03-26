package defaultconfig

import (
	"errors"
	"io"
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// Handler serves default-config APIs.
type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

type listPagesResponse struct {
	Pages []configdefaults.PageMetadata `json:"pages"`
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
	PageID   string                             `json:"pageId"`
	TabID    string                             `json:"tabId"`
	ID       string                             `json:"id"`
	Label    string                             `json:"label"`
	Order    int                                `json:"order"`
	Sections []configdefaults.SectionDefinition `json:"sections"`
	Panels   []configdefaults.PanelDefinition   `json:"panels"`
}

type savePageOverrideResponse struct {
	PageID  string `json:"pageId"`
	Message string `json:"message"`
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

// SavePageOverride saves a page-level override JSON blob.
func (h *Handler) SavePageOverride(c *gin.Context) {
	tenant := h.GetTenant(c)
	pageID := c.Param("pageId")
	if pageID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "pageId is required")
		return
	}

	payload, err := io.ReadAll(c.Request.Body)
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "invalid JSON override")
		return
	}

	override, err := configdefaults.DecodePageDocument(payload)
	if err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "invalid JSON override")
		return
	}

	if err := h.Service.SavePageOverride(tenant.TeamID, pageID, override); err != nil { //nolint:nestif
		status := http.StatusInternalServerError
		code := errorcode.Internal
		var httpErr httpError
		if errors.As(err, &httpErr) {
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
		modulecommon.RespondError(c, status, code, err.Error())
		return
	}

	modulecommon.RespondOK(c, savePageOverrideResponse{
		PageID:  pageID,
		Message: "Default config override saved successfully",
	})
}

type httpError string

func (e httpError) Error() string { return string(e) }
