package defaultconfig

import (
	"encoding/json"
	"fmt"
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
		if err := json.Unmarshal(pageBytes, &pageMeta); err != nil {
			return nil, fmt.Errorf("parse %s/page.json: %w", pageDir, err)
		}

		// Shell-only pages have no tabs directory — skip tab loading.
		if pageMeta.ShellOnly {
			doc := PageDocument{Page: pageMeta, Tabs: nil}
			if err := validatePageDocument(doc); err != nil {
				return nil, err
			}
			registry.pages[doc.Page.ID] = doc
			continue
		}

		tabEntries, err := fs.ReadDir(fsys, path.Join(pageDir, "tabs"))
		if err != nil {
			return nil, fmt.Errorf("read %s/tabs: %w", pageDir, err)
		}

		doc := PageDocument{
			Page: pageMeta,
			Tabs: make([]TabDefinition, 0, len(tabEntries)),
		}
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
			if err := json.Unmarshal(tabBytes, &tab); err != nil {
				return nil, fmt.Errorf("parse %s: %w", tabPath, err)
			}
			if tab.PageID == "" {
				tab.PageID = pageMeta.ID
			}
			if _, exists := seenTabs[tab.ID]; exists {
				return nil, fmt.Errorf("duplicate tab id %q for page %q", tab.ID, pageMeta.ID)
			}
			seenTabs[tab.ID] = struct{}{}

			seenComponents := map[string]struct{}{}
			for _, component := range tab.Components {
				if _, exists := seenComponents[component.ID]; exists {
					return nil, fmt.Errorf("duplicate component id %q for page %q tab %q", component.ID, pageMeta.ID, tab.ID)
				}
				seenComponents[component.ID] = struct{}{}
			}

			sort.SliceStable(tab.Components, func(i, j int) bool {
				if tab.Components[i].Order == tab.Components[j].Order {
					return tab.Components[i].ID < tab.Components[j].ID
				}
				return tab.Components[i].Order < tab.Components[j].Order
			})
			doc.Tabs = append(doc.Tabs, tab)
		}

		if err := validatePageDocument(doc); err != nil {
			return nil, err
		}

		sort.SliceStable(doc.Tabs, func(i, j int) bool {
			if doc.Tabs[i].Order == doc.Tabs[j].Order {
				return doc.Tabs[i].ID < doc.Tabs[j].ID
			}
			return doc.Tabs[i].Order < doc.Tabs[j].Order
		})

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
	bytes, err := json.Marshal(doc)
	if err != nil {
		return PageDocument{}, err
	}
	var cloned PageDocument
	if err := json.Unmarshal(bytes, &cloned); err != nil {
		return PageDocument{}, err
	}
	return cloned, nil
}

// MergePageDocument overlays saved page-level overrides on top of defaults.
func MergePageDocument(base PageDocument, override PageDocument) (PageDocument, error) {
	merged, err := ClonePageDocument(base)
	if err != nil {
		return PageDocument{}, err
	}

	if override.Page.ID != "" {
		page, err := mergeJSONStruct(merged.Page, override.Page)
		if err != nil {
			return PageDocument{}, err
		}
		merged.Page = page
	}

	if len(override.Tabs) > 0 {
		tabByID := make(map[string]TabDefinition, len(merged.Tabs)+len(override.Tabs))
		for _, tab := range merged.Tabs {
			tabByID[tab.ID] = tab
		}
		for _, tab := range override.Tabs {
			if tab.PageID == "" {
				tab.PageID = merged.Page.ID
			}
			if existing, ok := tabByID[tab.ID]; ok {
				mergedTab, err := mergeTabDefinition(existing, tab)
				if err != nil {
					return PageDocument{}, err
				}
				tabByID[tab.ID] = mergedTab
				continue
			}
			tabByID[tab.ID] = tab
		}

		merged.Tabs = merged.Tabs[:0]
		for _, tab := range tabByID {
			sort.SliceStable(tab.Components, func(i, j int) bool {
				if tab.Components[i].Order == tab.Components[j].Order {
					return tab.Components[i].ID < tab.Components[j].ID
				}
				return tab.Components[i].Order < tab.Components[j].Order
			})
			merged.Tabs = append(merged.Tabs, tab)
		}
		sort.SliceStable(merged.Tabs, func(i, j int) bool {
			if merged.Tabs[i].Order == merged.Tabs[j].Order {
				return merged.Tabs[i].ID < merged.Tabs[j].ID
			}
			return merged.Tabs[i].Order < merged.Tabs[j].Order
		})
	}

	if err := validatePageDocument(merged); err != nil {
		return PageDocument{}, err
	}
	return merged, nil
}

func mergeTabDefinition(base TabDefinition, override TabDefinition) (TabDefinition, error) {
	merged, err := mergeJSONStruct(base, override)
	if err != nil {
		return TabDefinition{}, err
	}

	if len(override.Groups) > 0 {
		groupByID := make(map[string]ComponentGroup, len(base.Groups)+len(override.Groups))
		for _, group := range base.Groups {
			groupByID[group.ID] = group
		}
		for _, group := range override.Groups {
			if existing, ok := groupByID[group.ID]; ok {
				mergedGroup, err := mergeJSONStruct(existing, group)
				if err != nil {
					return TabDefinition{}, err
				}
				groupByID[group.ID] = mergedGroup
				continue
			}
			groupByID[group.ID] = group
		}

		merged.Groups = merged.Groups[:0]
		for _, group := range groupByID {
			merged.Groups = append(merged.Groups, group)
		}
		sort.SliceStable(merged.Groups, func(i, j int) bool {
			if merged.Groups[i].Order == merged.Groups[j].Order {
				return merged.Groups[i].ID < merged.Groups[j].ID
			}
			return merged.Groups[i].Order < merged.Groups[j].Order
		})
	}

	if len(override.Components) > 0 {
		componentByID := make(map[string]Component, len(base.Components)+len(override.Components))
		for _, component := range base.Components {
			componentByID[component.ID] = component
		}
		for _, component := range override.Components {
			if existing, ok := componentByID[component.ID]; ok {
				mergedComponent, err := mergeJSONStruct(existing, component)
				if err != nil {
					return TabDefinition{}, err
				}
				componentByID[component.ID] = mergedComponent
				continue
			}
			componentByID[component.ID] = component
		}

		merged.Components = merged.Components[:0]
		for _, component := range componentByID {
			merged.Components = append(merged.Components, component)
		}
		sort.SliceStable(merged.Components, func(i, j int) bool {
			if merged.Components[i].Order == merged.Components[j].Order {
				return merged.Components[i].ID < merged.Components[j].ID
			}
			return merged.Components[i].Order < merged.Components[j].Order
		})
	}

	return merged, nil
}

func mergeJSONStruct[T any](base T, override T) (T, error) {
	baseMap, err := structToMap(base)
	if err != nil {
		var zero T
		return zero, err
	}
	overrideMap, err := structToMap(override)
	if err != nil {
		var zero T
		return zero, err
	}

	mergedMap := mergeJSONMaps(baseMap, overrideMap)
	mergedBytes, err := json.Marshal(mergedMap)
	if err != nil {
		var zero T
		return zero, err
	}

	var merged T
	if err := json.Unmarshal(mergedBytes, &merged); err != nil {
		var zero T
		return zero, err
	}
	return merged, nil
}

func structToMap(value any) (map[string]any, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var mapped map[string]any
	if err := json.Unmarshal(bytes, &mapped); err != nil {
		return nil, err
	}
	return mapped, nil
}

func mergeJSONMaps(base map[string]any, override map[string]any) map[string]any {
	merged := make(map[string]any, len(base)+len(override))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range override {
		if baseValue, ok := merged[key]; ok {
			baseMap, baseIsMap := baseValue.(map[string]any)
			overrideMap, overrideIsMap := value.(map[string]any)
			if baseIsMap && overrideIsMap {
				merged[key] = mergeJSONMaps(baseMap, overrideMap)
				continue
			}
		}
		merged[key] = value
	}
	return merged
}

func validatePageDocument(doc PageDocument) error {
	if doc.Page.ID == "" {
		return fmt.Errorf("page id is required")
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
	// Shell-only pages are rendered by a custom frontend component — no tabs needed.
	if doc.Page.ShellOnly {
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

		componentIDs := map[string]struct{}{}
		for _, component := range tab.Components {
			if err := validateComponent(doc.Page.ID, tab.ID, component, componentIDs); err != nil {
				return err
			}
		}
	}

	if !tabFound {
		return fmt.Errorf("page %q: defaultTabId %q not found", doc.Page.ID, doc.Page.DefaultTabID)
	}
	return nil
}

func validateComponent(pageID, tabID string, component Component, seenIDs map[string]struct{}) error {
	if component.ID == "" {
		return fmt.Errorf("page %q tab %q: component id is required", pageID, tabID)
	}
	if component.ComponentKey == "" {
		return fmt.Errorf("page %q tab %q component %q: componentKey is required", pageID, tabID, component.ID)
	}
	if _, exists := seenIDs[component.ID]; exists {
		return fmt.Errorf("page %q tab %q: duplicate component id %q", pageID, tabID, component.ID)
	}
	seenIDs[component.ID] = struct{}{}

	if component.Query.Endpoint == "" {
		return fmt.Errorf("page %q tab %q component %q: query.endpoint is required", pageID, tabID, component.ID)
	}
	if component.Query.Method == "" {
		return fmt.Errorf("page %q tab %q component %q: query.method is required", pageID, tabID, component.ID)
	}
	return nil
}

// GenerateDefaultDashboardConfigsJSON exports the entire registry of default pages as a JSON string.
// This is used to seed the dashboard_configs column when a new team is created.
func (r *Registry) GenerateDefaultDashboardConfigsJSON() (string, error) {
	if r == nil || len(r.pages) == 0 {
		return "{}", nil
	}

	bytes, err := json.Marshal(r.pages)
	if err != nil {
		return "", fmt.Errorf("failed to marshal default configs: %w", err)
	}

	return string(bytes), nil
}
