package dashboard

import (
	"log/slog"

	configdefaults "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg"
)

type Service struct {
	repo     Repository
	registry *configdefaults.Registry
}

func NewService(repo Repository, registry *configdefaults.Registry) *Service {
	return &Service{repo: repo, registry: registry}
}

func (s *Service) ListPages(teamID int64) []configdefaults.PageMetadata {
	defaultPages := s.registry.ListPages(true)
	pages := make([]configdefaults.PageMetadata, 0, len(defaultPages))
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

func (s *Service) ListTabs(teamID int64, pageID string) (configdefaults.PageDocument, error) {
	doc, ok := s.registry.GetPage(pageID)
	if !ok {
		return configdefaults.PageDocument{}, httpError("No configuration found for page: " + pageID)
	}
	return doc, nil
}

func (s *Service) GetTabDocument(teamID int64, pageID, tabID string) (configdefaults.TabDefinition, error) {
	doc, ok := s.registry.GetPage(pageID)
	if !ok {
		return configdefaults.TabDefinition{}, httpError("No configuration found for page: " + pageID)
	}

	for _, tab := range doc.Tabs {
		if tab.ID == tabID {
			return tab, nil
		}
	}

	return configdefaults.TabDefinition{}, httpError("No tab found for page: " + pageID + " tab: " + tabID)
}

type httpError string

func (e httpError) Error() string { return string(e) }
