package dashboard

import (
	"encoding/json"

	configdefaults "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"go.uber.org/zap"
)

type Service struct {
	repo        Repository
	registry    *configdefaults.Registry
	useDefaults bool
}

func NewService(repo Repository, registry *configdefaults.Registry, useDefaults bool) *Service {
	return &Service{repo: repo, registry: registry, useDefaults: useDefaults}
}

func (s *Service) ListPages(teamID int64) []configdefaults.PageMetadata {
	defaultPages := s.registry.ListPages(true)
	pages := make([]configdefaults.PageMetadata, 0, len(defaultPages))
	for _, page := range defaultPages {
		doc, err := s.resolvePage(teamID, page.ID)
		if err != nil {
			logger.L().Warn("default-config: failed to resolve page", zap.String("page_id", page.ID), zap.Int64("team_id", teamID), zap.Error(err))
			continue
		}
		if doc.Page.Navigable {
			pages = append(pages, doc.Page)
		}
	}
	return pages
}

func (s *Service) ListTabs(teamID int64, pageID string) (configdefaults.PageDocument, error) {
	return s.resolvePage(teamID, pageID)
}

func (s *Service) GetTabDocument(teamID int64, pageID, tabID string) (configdefaults.TabDefinition, error) {
	doc, err := s.resolvePage(teamID, pageID)
	if err != nil {
		return configdefaults.TabDefinition{}, err
	}

	for _, tab := range doc.Tabs {
		if tab.ID == tabID {
			return tab, nil
		}
	}

	return configdefaults.TabDefinition{}, httpError("No tab found for page: " + pageID + " tab: " + tabID)
}

func (s *Service) resolvePage(teamID int64, pageID string) (configdefaults.PageDocument, error) {
	defaultDoc, ok := s.registry.GetPage(pageID)
	if !ok {
		return configdefaults.PageDocument{}, httpError("No configuration found for page: " + pageID)
	}

	if s.useDefaults {
		return defaultDoc, nil
	}

	override, err := s.repo.GetPageOverride(teamID, pageID)
	if err != nil || override.ConfigJSON == "" {
		s.seedDefaultDoc(teamID, pageID, defaultDoc)
		return defaultDoc, nil //nolint:nilerr // missing override is expected; fall back to defaults
	}

	pageOverride, err := configdefaults.DecodePageDocument([]byte(override.ConfigJSON))
	if err != nil {
		logger.L().Warn("default-config: ignoring invalid override", zap.String("page_id", pageID), zap.Int64("team_id", teamID), zap.Error(err))
		s.seedDefaultDoc(teamID, pageID, defaultDoc)
		return defaultDoc, nil
	}

	if err := normalizeOverridePageDocument(&pageOverride, defaultDoc); err != nil {
		logger.L().Warn("default-config: ignoring invalid override metadata", zap.String("page_id", pageID), zap.Int64("team_id", teamID), zap.Error(err))
		s.seedDefaultDoc(teamID, pageID, defaultDoc)
		return defaultDoc, nil
	}

	if err := configdefaults.ValidatePageDocument(pageOverride); err != nil {
		logger.L().Warn("default-config: ignoring invalid override document", zap.String("page_id", pageID), zap.Int64("team_id", teamID), zap.Error(err))
		s.seedDefaultDoc(teamID, pageID, defaultDoc)
		return defaultDoc, nil
	}
	return pageOverride, nil
}

func (s *Service) seedDefaultDoc(teamID int64, pageID string, defaultDoc configdefaults.PageDocument) {
	seedBytes, marshalErr := json.Marshal(defaultDoc)
	if marshalErr != nil {
		return
	}
	if saveErr := s.repo.SavePageOverride(teamID, pageID, string(seedBytes)); saveErr != nil {
		logger.L().Warn("default-config: failed to seed page", zap.String("page_id", pageID), zap.Int64("team_id", teamID), zap.Error(saveErr))
	}
}

func normalizeOverridePageDocument(
	override *configdefaults.PageDocument,
	defaultDoc configdefaults.PageDocument,
) error {
	if override == nil {
		return httpError("page override payload is required")
	}

	if override.Page.SchemaVersion == 0 {
		override.Page.SchemaVersion = defaultDoc.Page.SchemaVersion
	}
	if override.Page.ID != "" && override.Page.ID != defaultDoc.Page.ID {
		return httpError("page.id must match the route pageId")
	}
	if override.Page.ID == "" {
		override.Page.ID = defaultDoc.Page.ID
	}
	if override.Page.Path == "" {
		override.Page.Path = defaultDoc.Page.Path
	}

	for i := range override.Tabs {
		if override.Tabs[i].PageID == "" {
			override.Tabs[i].PageID = defaultDoc.Page.ID
		}
	}

	return nil
}

type httpError string

func (e httpError) Error() string { return string(e) }
