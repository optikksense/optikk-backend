package dashboardcfg

import (
	"errors"
	"fmt"
)

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
	for _, column := range panel.Columns {
		if column.Key == "" {
			return fmt.Errorf("page %q tab %q panel %q: column key is required", pageID, tabID, panel.ID)
		}
		if column.Label == "" {
			return fmt.Errorf("page %q tab %q panel %q: column label is required", pageID, tabID, panel.ID)
		}
		if column.Align != "" {
			if _, ok := allowedColumnAligns[column.Align]; !ok {
				return fmt.Errorf("page %q tab %q panel %q: unsupported column align %q", pageID, tabID, panel.ID, column.Align)
			}
		}
		if column.Width != nil && *column.Width <= 0 {
			return fmt.Errorf("page %q tab %q panel %q: column width must be > 0", pageID, tabID, panel.ID)
		}
	}
	if panel.DrawerAction != nil {
		if _, ok := allowedDrawerEntities[panel.DrawerAction.Entity]; !ok {
			return fmt.Errorf("page %q tab %q panel %q: unsupported drawer entity %q", pageID, tabID, panel.ID, panel.DrawerAction.Entity)
		}
		if panel.DrawerAction.IDField == "" {
			return fmt.Errorf("page %q tab %q panel %q: drawerAction.idField is required", pageID, tabID, panel.ID)
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
