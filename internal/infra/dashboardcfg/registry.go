package dashboardcfg

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// Registry holds the set of default dashboard page configurations.
type Registry struct {
	pages map[string]PageDocument
}

// LoadFromDocuments creates a Registry from a slice of PageDocument values.
func LoadFromDocuments(documents []PageDocument) (*Registry, error) {
	registry := &Registry{pages: make(map[string]PageDocument, len(documents))}
	for _, doc := range documents {
		if err := registerPageDocument(registry, doc); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

// GetPage returns a deep clone of the page with the given ID.
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

// ListPages returns metadata for all registered pages, sorted by order then ID.
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

// ClonePageDocument performs a deep clone of a PageDocument via JSON round-trip.
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

// ValidatePageDocument is the public entry point for page document validation.
func ValidatePageDocument(doc PageDocument) error {
	return validatePageDocument(doc)
}



func registerPageDocument(registry *Registry, doc PageDocument) error {
	if registry == nil {
		return errors.New("registry is required")
	}
	normalized, err := preparePageDocument(doc)
	if err != nil {
		return err
	}
	if _, exists := registry.pages[normalized.Page.ID]; exists {
		return fmt.Errorf("duplicate page id %q", normalized.Page.ID)
	}
	registry.pages[normalized.Page.ID] = normalized
	return nil
}

func preparePageDocument(doc PageDocument) (PageDocument, error) {
	if doc.Page.RenderMode == RenderModeDashboard {
		for i := range doc.Tabs {
			sortSections(doc.Tabs[i].Sections)
			sortPanels(doc.Tabs[i].Panels)
			if err := compileTabLayouts(&doc.Tabs[i]); err != nil {
				return PageDocument{}, fmt.Errorf("compile page %q tab %q: %w", doc.Page.ID, doc.Tabs[i].ID, err)
			}
		}
		sortTabs(doc.Tabs)
	}
	if err := validatePageDocument(doc); err != nil {
		return PageDocument{}, err
	}
	return doc, nil
}
