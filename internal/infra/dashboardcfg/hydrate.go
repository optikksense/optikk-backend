package dashboardcfg

import (
	"fmt"
	"sort"
)

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
