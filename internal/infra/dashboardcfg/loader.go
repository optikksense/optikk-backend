package dashboardcfg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
)

type Registry struct {
	pages map[string]PageDocument
}

func LoadFromFS(fsys fs.FS) (*Registry, error) {
	pageEntries, err := fs.ReadDir(fsys, "pages")
	if err != nil {
		return nil, fmt.Errorf("read pages directory: %w", err)
	}

	registry := &Registry{pages: make(map[string]PageDocument)}
	for _, entry := range pageEntries {
		if !entry.IsDir() {
			continue
		}

		pageDir := path.Join("pages", entry.Name())
		pageBytes, err := fs.ReadFile(fsys, path.Join(pageDir, "page.json"))
		if err != nil {
			return nil, fmt.Errorf("read %s/page.json: %w", pageDir, err)
		}

		var pageMeta PageMetadata
		if err := strictUnmarshal(pageBytes, &pageMeta); err != nil {
			return nil, fmt.Errorf("parse %s/page.json: %w", pageDir, err)
		}

		doc := PageDocument{Page: pageMeta}
		if pageMeta.RenderMode == RenderModeDashboard { //nolint:nestif
			tabEntries, err := fs.ReadDir(fsys, path.Join(pageDir, "tabs"))
			if err != nil {
				return nil, fmt.Errorf("read %s/tabs: %w", pageDir, err)
			}

			doc.Tabs = make([]TabDefinition, 0, len(tabEntries))
			seenTabs := map[string]struct{}{}
			for _, tabEntry := range tabEntries {
				if tabEntry.IsDir() || !strings.HasSuffix(tabEntry.Name(), ".json") {
					continue
				}

				tabPath := path.Join(pageDir, "tabs", tabEntry.Name())
				tabBytes, err := fs.ReadFile(fsys, tabPath)
				if err != nil {
					return nil, fmt.Errorf("read %s: %w", tabPath, err)
				}

				var tab TabDefinition
				if err := strictUnmarshal(tabBytes, &tab); err != nil {
					return nil, fmt.Errorf("parse %s: %w", tabPath, err)
				}
				if tab.PageID == "" {
					tab.PageID = pageMeta.ID
				}
				if _, exists := seenTabs[tab.ID]; exists {
					return nil, fmt.Errorf("duplicate tab id %q for page %q", tab.ID, pageMeta.ID)
				}
				seenTabs[tab.ID] = struct{}{}

				sortSections(tab.Sections)
				sortPanels(tab.Panels)
				doc.Tabs = append(doc.Tabs, tab)
			}
			sortTabs(doc.Tabs)
		}

		if err := validatePageDocument(doc); err != nil {
			return nil, err
		}

		if _, exists := registry.pages[doc.Page.ID]; exists {
			return nil, fmt.Errorf("duplicate page id %q", doc.Page.ID)
		}
		registry.pages[doc.Page.ID] = doc
	}

	return registry, nil
}

func (r *Registry) GetPage(pageID string) (PageDocument, bool) {
	if r == nil {
		return PageDocument{}, false
	}
	doc, ok := r.pages[pageID]
	if !ok {
		return PageDocument{}, false
	}
	cloned, err := ClonePageDocument(doc)
	if err != nil {
		return PageDocument{}, false
	}
	return cloned, true
}

func (r *Registry) ListPages(navigableOnly bool) []PageMetadata {
	if r == nil {
		return nil
	}

	pages := make([]PageMetadata, 0, len(r.pages))
	for _, doc := range r.pages {
		if navigableOnly && !doc.Page.Navigable {
			continue
		}
		pages = append(pages, doc.Page)
	}

	sort.SliceStable(pages, func(i, j int) bool {
		if pages[i].Order == pages[j].Order {
			return pages[i].ID < pages[j].ID
		}
		return pages[i].Order < pages[j].Order
	})
	return pages
}

func ClonePageDocument(doc PageDocument) (PageDocument, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return PageDocument{}, err
	}
	var cloned PageDocument
	if err := json.Unmarshal(data, &cloned); err != nil {
		return PageDocument{}, err
	}
	return cloned, nil
}

func ValidatePageDocument(doc PageDocument) error {
	return validatePageDocument(doc)
}

func DecodePageDocument(data []byte) (PageDocument, error) {
	var doc PageDocument
	if err := strictUnmarshal(data, &doc); err != nil {
		return PageDocument{}, err
	}
	return doc, nil
}

func validatePageDocument(doc PageDocument) error {
	if doc.Page.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf(
			"page %q: schemaVersion must be %d",
			doc.Page.ID,
			CurrentSchemaVersion,
		)
	}
	if doc.Page.ID == "" {
		return errors.New("page id is required")
	}
	if doc.Page.Path == "" {
		return fmt.Errorf("page %q: path is required", doc.Page.ID)
	}
	if doc.Page.Label == "" {
		return fmt.Errorf("page %q: label is required", doc.Page.ID)
	}
	if doc.Page.Icon == "" {
		return fmt.Errorf("page %q: icon is required", doc.Page.ID)
	}
	if doc.Page.Group == "" {
		return fmt.Errorf("page %q: group is required", doc.Page.ID)
	}
	if doc.Page.RenderMode != RenderModeDashboard && doc.Page.RenderMode != RenderModeExplorer {
		return fmt.Errorf("page %q: renderMode must be %q or %q", doc.Page.ID, RenderModeDashboard, RenderModeExplorer)
	}
	if doc.Page.RenderMode == RenderModeExplorer {
		if len(doc.Tabs) > 0 {
			return fmt.Errorf("page %q: explorer pages must not define tabs", doc.Page.ID)
		}
		return nil
	}
	if doc.Page.DefaultTabID == "" {
		return fmt.Errorf("page %q: defaultTabId is required", doc.Page.ID)
	}
	if len(doc.Tabs) == 0 {
		return fmt.Errorf("page %q: at least one tab is required", doc.Page.ID)
	}

	tabFound := false
	tabIDs := map[string]struct{}{}
	for _, tab := range doc.Tabs {
		if tab.ID == "" {
			return fmt.Errorf("page %q: tab id is required", doc.Page.ID)
		}
		if tab.PageID == "" {
			return fmt.Errorf("page %q tab %q: pageId is required", doc.Page.ID, tab.ID)
		}
		if tab.PageID != doc.Page.ID {
			return fmt.Errorf("page %q tab %q: pageId mismatch", doc.Page.ID, tab.ID)
		}
		if tab.Label == "" {
			return fmt.Errorf("page %q tab %q: label is required", doc.Page.ID, tab.ID)
		}
		if _, exists := tabIDs[tab.ID]; exists {
			return fmt.Errorf("page %q: duplicate tab id %q", doc.Page.ID, tab.ID)
		}
		tabIDs[tab.ID] = struct{}{}
		if tab.ID == doc.Page.DefaultTabID {
			tabFound = true
		}
		if len(tab.Sections) == 0 {
			return fmt.Errorf("page %q tab %q: at least one section is required", doc.Page.ID, tab.ID)
		}
		if len(tab.Panels) == 0 {
			return fmt.Errorf("page %q tab %q: at least one panel is required", doc.Page.ID, tab.ID)
		}

		sectionIDs := map[string]struct{}{}
		sectionsByID := make(map[string]SectionDefinition, len(tab.Sections))
		for _, section := range tab.Sections {
			if err := validateSection(doc.Page.ID, tab.ID, section, sectionIDs); err != nil {
				return err
			}
			sectionsByID[section.ID] = section
		}

		panelIDs := map[string]struct{}{}
		sectionUsage := make(map[string]int, len(tab.Sections))
		panelsBySection := make(map[string][]PanelDefinition, len(tab.Sections))
		for _, panel := range tab.Panels {
			if err := validatePanel(doc.Page.ID, tab.ID, panel, panelIDs, sectionIDs); err != nil {
				return err
			}
			sectionUsage[panel.SectionID]++
			panelsBySection[panel.SectionID] = append(panelsBySection[panel.SectionID], panel)
		}

		for _, section := range tab.Sections {
			if sectionUsage[section.ID] == 0 {
				return fmt.Errorf("page %q tab %q section %q: must contain at least one panel", doc.Page.ID, tab.ID, section.ID)
			}
			if err := validateSectionComposition(doc.Page.ID, tab.ID, sectionsByID[section.ID], panelsBySection[section.ID]); err != nil {
				return err
			}
		}
	}

	if !tabFound {
		return fmt.Errorf("page %q: defaultTabId %q not found", doc.Page.ID, doc.Page.DefaultTabID)
	}
	return nil
}

func validateSection(pageID, tabID string, section SectionDefinition, seenIDs map[string]struct{}) error {
	if section.ID == "" {
		return fmt.Errorf("page %q tab %q: section id is required", pageID, tabID)
	}
	if section.Title == "" {
		return fmt.Errorf("page %q tab %q section %q: title is required", pageID, tabID, section.ID)
	}
	if section.Kind != SectionKindSummary &&
		section.Kind != SectionKindTrends &&
		section.Kind != SectionKindBreakdowns &&
		section.Kind != SectionKindDetails {
		return fmt.Errorf("page %q tab %q section %q: invalid kind %q", pageID, tabID, section.ID, section.Kind)
	}
	if section.LayoutMode != SectionLayoutModeKPIStrip &&
		section.LayoutMode != SectionLayoutModeTwoUp &&
		section.LayoutMode != SectionLayoutModeThreeUp &&
		section.LayoutMode != SectionLayoutModeStack {
		return fmt.Errorf("page %q tab %q section %q: invalid layoutMode %q", pageID, tabID, section.ID, section.LayoutMode)
	}
	if _, exists := seenIDs[section.ID]; exists {
		return fmt.Errorf("page %q tab %q: duplicate section id %q", pageID, tabID, section.ID)
	}
	seenIDs[section.ID] = struct{}{}
	return nil
}

func validatePanel(pageID, tabID string, panel PanelDefinition, seenIDs map[string]struct{}, sections map[string]struct{}) error {
	if panel.ID == "" {
		return fmt.Errorf("page %q tab %q: panel id is required", pageID, tabID)
	}
	if panel.PanelType == "" {
		return fmt.Errorf("page %q tab %q panel %q: panelType is required", pageID, tabID, panel.ID)
	}
	if _, allowed := allowedPanelTypes[panel.PanelType]; !allowed {
		return fmt.Errorf("page %q tab %q panel %q: unsupported panelType %q", pageID, tabID, panel.ID, panel.PanelType)
	}
	if panel.SectionID == "" {
		return fmt.Errorf("page %q tab %q panel %q: sectionId is required", pageID, tabID, panel.ID)
	}
	if _, exists := sections[panel.SectionID]; !exists {
		return fmt.Errorf("page %q tab %q panel %q: unknown sectionId %q", pageID, tabID, panel.ID, panel.SectionID)
	}
	if _, exists := seenIDs[panel.ID]; exists {
		return fmt.Errorf("page %q tab %q: duplicate panel id %q", pageID, tabID, panel.ID)
	}
	seenIDs[panel.ID] = struct{}{}

	if panel.Query.Endpoint == "" {
		return fmt.Errorf("page %q tab %q panel %q: query.endpoint is required", pageID, tabID, panel.ID)
	}
	if panel.Query.Method == "" {
		return fmt.Errorf("page %q tab %q panel %q: query.method is required", pageID, tabID, panel.ID)
	}
	if panel.Layout.Preset != PanelPresetKPI &&
		panel.Layout.Preset != PanelPresetTrend &&
		panel.Layout.Preset != PanelPresetHero &&
		panel.Layout.Preset != PanelPresetBreakdown &&
		panel.Layout.Preset != PanelPresetDetail {
		return fmt.Errorf("page %q tab %q panel %q: invalid layout.preset %q", pageID, tabID, panel.ID, panel.Layout.Preset)
	}
	if panel.Layout.X != nil || panel.Layout.Y != nil || panel.Layout.W != nil {
		return fmt.Errorf("page %q tab %q panel %q: manual layout coordinates are not supported", pageID, tabID, panel.ID)
	}
	if panel.Layout.H != nil && *panel.Layout.H <= 0 {
		return fmt.Errorf("page %q tab %q panel %q: layout.h must be > 0", pageID, tabID, panel.ID)
	}
	if panel.Layout.ColSpan != nil {
		cs := *panel.Layout.ColSpan
		if cs < 1 || cs > 4 {
			return fmt.Errorf("page %q tab %q panel %q: layout.colSpan must be between 1 and 4", pageID, tabID, panel.ID)
		}
	}
	return nil
}

func validateSectionComposition(
	pageID, tabID string,
	section SectionDefinition,
	panels []PanelDefinition,
) error {
	if section.Kind == SectionKindSummary && section.LayoutMode != SectionLayoutModeKPIStrip {
		return fmt.Errorf(
			"page %q tab %q section %q: summary sections must use layoutMode %q",
			pageID,
			tabID,
			section.ID,
			SectionLayoutModeKPIStrip,
		)
	}

	if section.Kind == SectionKindDetails &&
		section.LayoutMode != SectionLayoutModeStack &&
		section.LayoutMode != SectionLayoutModeTwoUp &&
		section.LayoutMode != SectionLayoutModeThreeUp {
		return fmt.Errorf(
			"page %q tab %q section %q: details sections must use layoutMode %q, %q, or %q",
			pageID,
			tabID,
			section.ID,
			SectionLayoutModeStack,
			SectionLayoutModeTwoUp,
			SectionLayoutModeThreeUp,
		)
	}

	if section.LayoutMode == SectionLayoutModeKPIStrip {
		for _, panel := range panels {
			if panel.PanelType != "stat-card" && panel.PanelType != "stat-summary" {
				return fmt.Errorf(
					"page %q tab %q section %q: kpi-strip sections only allow stat-card or stat-summary panels",
					pageID,
					tabID,
					section.ID,
				)
			}
		}
		return nil
	}

	for _, panel := range panels {
		if panel.PanelType == "stat-card" || panel.PanelType == "stat-summary" {
			return fmt.Errorf(
				"page %q tab %q section %q: KPI panels must live in kpi-strip sections",
				pageID,
				tabID,
				section.ID,
			)
		}
	}

	return nil
}

func sortTabs(tabs []TabDefinition) {
	sort.SliceStable(tabs, func(i, j int) bool {
		if tabs[i].Order == tabs[j].Order {
			return tabs[i].ID < tabs[j].ID
		}
		return tabs[i].Order < tabs[j].Order
	})
}

func sortSections(sections []SectionDefinition) {
	sort.SliceStable(sections, func(i, j int) bool {
		if sections[i].Order == sections[j].Order {
			return sections[i].ID < sections[j].ID
		}
		return sections[i].Order < sections[j].Order
	})
}

func sortPanels(panels []PanelDefinition) {
	sort.SliceStable(panels, func(i, j int) bool {
		if panels[i].Order == panels[j].Order {
			return panels[i].ID < panels[j].ID
		}
		return panels[i].Order < panels[j].Order
	})
}

// GenerateDefaultDashboardConfigsJSON exports the entire registry of default pages as a JSON string.
// This is used to seed the dashboard_configs column when a new team is created.
func (r *Registry) GenerateDefaultDashboardConfigsJSON() (string, error) {
	if r == nil || len(r.pages) == 0 {
		return "{}", nil
	}

	data, err := json.Marshal(r.pages)
	if err != nil {
		return "", fmt.Errorf("failed to marshal default configs: %w", err)
	}

	return string(data), nil
}

func strictUnmarshal(data []byte, dest any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dest); err != nil {
		return err
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("unexpected trailing JSON content")
	}

	return nil
}
