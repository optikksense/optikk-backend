package dashboardcfg

import "log/slog"

type Service struct {
	registry *Registry
}

func NewService(registry *Registry) *Service {
	return &Service{registry: registry}
}

func (s *Service) ListPages(teamID int64) []PageMetadata {
	defaultPages := s.registry.ListPages(true)
	pages := make([]PageMetadata, 0, len(defaultPages))
	for _, page := range defaultPages {
		doc, ok := s.registry.GetPage(page.ID)
		if !ok {
			slog.Warn("default-config: failed to resolve page", slog.String("page_id", page.ID), slog.Int64("team_id", teamID))
			continue
		}
		if doc.Page.Navigable {
			pages = append(pages, doc.Page)
		}
	}
	return pages
}

func (s *Service) ListTabs(teamID int64, pageID string) (PageDocument, error) {
	doc, ok := s.registry.GetPage(pageID)
	if !ok {
		return PageDocument{}, httpError("No configuration found for page: " + pageID)
	}
	return doc, nil
}

func (s *Service) GetTabDocument(teamID int64, pageID, tabID string) (TabDefinition, error) {
	doc, ok := s.registry.GetPage(pageID)
	if !ok {
		return TabDefinition{}, httpError("No configuration found for page: " + pageID)
	}

	for _, tab := range doc.Tabs {
		if tab.ID == tabID {
			return tab, nil
		}
	}

	return TabDefinition{}, httpError("No tab found for page: " + pageID + " tab: " + tabID)
}

type httpError string

func (e httpError) Error() string { return string(e) }
