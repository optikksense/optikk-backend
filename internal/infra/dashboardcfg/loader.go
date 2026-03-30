package dashboardcfg

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
)

var allowedFormatters = map[string]struct{}{
	"ms":       {},
	"ns":       {},
	"bytes":    {},
	"percent1": {},
	"percent2": {},
	"number":   {},
}

var drilldownPlaceholderPattern = regexp.MustCompile(`\{([^}]+)\}`)

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
				if err := compileTabLayouts(&tab); err != nil {
					return nil, fmt.Errorf("compile %s: %w", tabPath, err)
				}
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
	if doc.Page.SchemaVersion < MinSupportedSchemaVersion || doc.Page.SchemaVersion > CurrentSchemaVersion {
		return fmt.Errorf(
			"page %q: schemaVersion must be between %d and %d",
			doc.Page.ID,
			MinSupportedSchemaVersion,
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

		if err := hydrateTabPanelLayouts(tab.ID, &tab.Panels); err != nil {
			return err
		}

		sectionIDs := map[string]struct{}{}
		for _, section := range tab.Sections {
			if err := validateSection(doc.Page.ID, tab.ID, section, sectionIDs); err != nil {
				return err
			}
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
			if err := validateSectionPanelLayouts(doc.Page.ID, tab.ID, section, panelsBySection[section.ID]); err != nil {
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
	if section.SectionTemplate == "" {
		return fmt.Errorf("page %q tab %q section %q: sectionTemplate is required", pageID, tabID, section.ID)
	}
	if _, allowed := allowedSectionTemplates[section.SectionTemplate]; !allowed {
		return fmt.Errorf("page %q tab %q section %q: unsupported sectionTemplate %q", pageID, tabID, section.ID, section.SectionTemplate)
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
	if panel.LayoutVariant == "" {
		return fmt.Errorf("page %q tab %q panel %q: layoutVariant is required", pageID, tabID, panel.ID)
	}
	if _, allowed := allowedPanelTypes[panel.PanelType]; !allowed {
		return fmt.Errorf("page %q tab %q panel %q: unsupported panelType %q", pageID, tabID, panel.ID, panel.PanelType)
	}
	if _, allowed := allowedLayoutVariants[panel.LayoutVariant]; !allowed {
		return fmt.Errorf("page %q tab %q panel %q: unsupported layoutVariant %q", pageID, tabID, panel.ID, panel.LayoutVariant)
	}
	if !isLayoutVariantAllowed(panel.PanelType, panel.LayoutVariant) {
		return fmt.Errorf(
			"page %q tab %q panel %q: layoutVariant %q is not allowed for panelType %q",
			pageID,
			tabID,
			panel.ID,
			panel.LayoutVariant,
			panel.PanelType,
		)
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
	if panel.Layout.X < 0 {
		return fmt.Errorf("page %q tab %q panel %q: layout.x must be >= 0", pageID, tabID, panel.ID)
	}
	if panel.Layout.Y < 0 {
		return fmt.Errorf("page %q tab %q panel %q: layout.y must be >= 0", pageID, tabID, panel.ID)
	}
	if panel.Layout.W <= 0 {
		return fmt.Errorf("page %q tab %q panel %q: layout.w must be > 0", pageID, tabID, panel.ID)
	}
	if panel.Layout.H <= 0 {
		return fmt.Errorf("page %q tab %q panel %q: layout.h must be > 0", pageID, tabID, panel.ID)
	}
	if err := ValidateLayoutMatchesVariant(panel.LayoutVariant, panel.Layout.W, panel.Layout.H); err != nil {
		return fmt.Errorf("page %q tab %q panel %q: %w", pageID, tabID, panel.ID, err)
	}
	if panel.Layout.X+panel.Layout.W > gridCols {
		return fmt.Errorf("page %q tab %q panel %q: layout.x + layout.w must be <= %d", pageID, tabID, panel.ID, gridCols)
	}
	if panel.Formatter != "" {
		if _, ok := allowedFormatters[panel.Formatter]; !ok {
			return fmt.Errorf("page %q tab %q panel %q: unsupported formatter %q", pageID, tabID, panel.ID, panel.Formatter)
		}
	}
	return validatePanelDataContract(pageID, tabID, panel)
}

func validatePanelDataContract(pageID, tabID string, panel PanelDefinition) error {
	if _, ok := dashboardEndpointFieldContracts[panel.Query.Endpoint]; !ok {
		return fmt.Errorf(
			"page %q tab %q panel %q: endpoint %q has no declared dashboard field contract",
			pageID,
			tabID,
			panel.ID,
			panel.Query.Endpoint,
		)
	}

	checkField := func(fieldName, fieldValue string) error {
		if fieldValue == "" {
			return nil
		}
		if endpointContractHasField(panel.Query.Endpoint, fieldValue) {
			return nil
		}
		return fmt.Errorf(
			"page %q tab %q panel %q: %s %q is not declared for endpoint %q",
			pageID,
			tabID,
			panel.ID,
			fieldName,
			fieldValue,
			panel.Query.Endpoint,
		)
	}

	fields := []struct {
		name  string
		value string
	}{
		{name: "groupByKey", value: panel.GroupByKey},
		{name: "labelKey", value: panel.LabelKey},
		{name: "valueField", value: panel.ValueField},
		{name: "valueKey", value: panel.ValueKey},
		{name: "bucketKey", value: panel.BucketKey},
		{name: "xKey", value: panel.XKey},
		{name: "yKey", value: panel.YKey},
		{name: "listSortField", value: panel.ListSortField},
	}
	for _, field := range fields {
		if err := checkField(field.name, field.value); err != nil {
			return err
		}
	}
	for _, valueKey := range panel.ValueKeys {
		if err := checkField("valueKeys", valueKey); err != nil {
			return err
		}
	}
	for _, summaryField := range panel.SummaryFields {
		if err := checkField("summaryFields.field", summaryField.Field); err != nil {
			return err
		}
		for _, key := range summaryField.Keys {
			if err := checkField("summaryFields.keys", key); err != nil {
				return err
			}
		}
	}
	for _, match := range drilldownPlaceholderPattern.FindAllStringSubmatch(panel.DrilldownRoute, -1) {
		if len(match) < 2 {
			continue
		}
		if err := checkField("drilldownRoute placeholder", match[1]); err != nil {
			return err
		}
	}
	return nil
}

func validateSectionPanelLayouts(
	pageID, tabID string,
	section SectionDefinition,
	panels []PanelDefinition,
) error {
	hydrated := append([]PanelDefinition(nil), panels...)
	if err := hydrateTabPanelLayouts(tabID, &hydrated); err != nil {
		return fmt.Errorf("page %q tab %q section %q: %w", pageID, tabID, section.ID, err)
	}

	canonicalLayouts, err := canonicalSectionLayout(section, hydrated)
	if err != nil {
		return fmt.Errorf("page %q tab %q section %q: %w", pageID, tabID, section.ID, err)
	}

	for _, panel := range hydrated {
		want := canonicalLayouts[panel.ID]
		if panel.Layout != want {
			return fmt.Errorf(
				"page %q tab %q section %q panel %q: layout must match canonical section packing; got layout=%+v, want layout=%+v for pattern %q",
				pageID,
				tabID,
				section.ID,
				panel.ID,
				panel.Layout,
				want,
				sectionLayoutPattern(section, hydrated),
			)
		}
	}

	occupied := make(map[string]string)
	for _, panel := range hydrated {
		for x := panel.Layout.X; x < panel.Layout.X+panel.Layout.W; x++ {
			for y := panel.Layout.Y; y < panel.Layout.Y+panel.Layout.H; y++ {
				key := fmt.Sprintf("%d:%d", x, y)
				if existing, exists := occupied[key]; exists {
					return fmt.Errorf(
						"page %q tab %q section %q: panels %q and %q overlap at (%d,%d)",
						pageID,
						tabID,
						section.ID,
						existing,
						panel.ID,
						x,
						y,
					)
				}
				occupied[key] = panel.ID
			}
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

// hydrateTabPanelLayouts sets layout.w and layout.h from the layoutVariant footprint when either
// dimension is missing (legacy JSON without layout dimensions).
func hydrateTabPanelLayouts(tabID string, panels *[]PanelDefinition) error {
	if panels == nil {
		return nil
	}
	s := *panels
	for i := range s {
		p := &s[i]
		if p.Layout.W <= 0 || p.Layout.H <= 0 {
			size, err := panelSizeForVariant(p.LayoutVariant)
			if err != nil {
				return fmt.Errorf("tab %q panel %q: %w", tabID, p.ID, err)
			}
			p.Layout.W = size.W
			p.Layout.H = size.H
		}
	}
	return nil
}

func compileTabLayouts(tab *TabDefinition) error {
	if err := hydrateTabPanelLayouts(tab.ID, &tab.Panels); err != nil {
		return err
	}

	sectionsByID := make(map[string]SectionDefinition, len(tab.Sections))
	for _, section := range tab.Sections {
		sectionsByID[section.ID] = section
	}

	panelsBySection := make(map[string][]*PanelDefinition, len(tab.Sections))
	for index := range tab.Panels {
		panel := &tab.Panels[index]
		panelsBySection[panel.SectionID] = append(panelsBySection[panel.SectionID], panel)
	}

	for _, section := range tab.Sections {
		panelRefs := panelsBySection[section.ID]
		if len(panelRefs) == 0 {
			continue
		}

		panels := make([]PanelDefinition, 0, len(panelRefs))
		for _, panelRef := range panelRefs {
			panels = append(panels, *panelRef)
		}

		layouts, err := canonicalSectionLayout(section, panels)
		if err != nil {
			return fmt.Errorf("tab %q section %q: %w", tab.ID, section.ID, err)
		}
		for _, panelRef := range panelRefs {
			panelRef.Layout = layouts[panelRef.ID]
		}
	}

	return nil
}
