package defaultconfig

import (
	"encoding/json"
	"log"

	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
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
			log.Printf("default-config: failed to resolve page %s for team %d: %v", page.ID, teamID, err)
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

func (s *Service) ListComponents(teamID int64, pageID string) (configdefaults.PageDocument, error) {
	return s.resolvePage(teamID, pageID)
}

func (s *Service) SavePageOverride(teamID int64, pageID string, override configdefaults.PageDocument) error {
	defaultDoc, ok := s.registry.GetPage(pageID)
	if !ok {
		return httpError("No configuration found for page: " + pageID)
	}

	if override.Page.ID != "" && override.Page.ID != pageID {
		return httpError("page.id must match the route pageId")
	}
	if override.Page.ID == "" {
		override.Page.ID = pageID
	}
	for i := range override.Tabs {
		if override.Tabs[i].PageID == "" {
			override.Tabs[i].PageID = pageID
		}
	}

	if _, err := configdefaults.MergePageDocument(defaultDoc, override); err != nil {
		return err
	}

	bytes, err := json.Marshal(override)
	if err != nil {
		return err
	}
	return s.repo.SavePageOverride(teamID, pageID, string(bytes))
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
		if seedBytes, marshalErr := json.Marshal(defaultDoc); marshalErr == nil {
			if saveErr := s.repo.SavePageOverride(teamID, pageID, string(seedBytes)); saveErr != nil {
				log.Printf("default-config: failed to seed page=%s for team=%d: %v", pageID, teamID, saveErr)
			}
		}
		return defaultDoc, nil
	}

	var pageOverride configdefaults.PageDocument
	if err := json.Unmarshal([]byte(override.ConfigJSON), &pageOverride); err != nil {
		log.Printf("default-config: ignoring invalid override for page=%s team=%d: %v", pageID, teamID, err)
		return defaultDoc, nil
	}

	merged, err := configdefaults.MergePageDocument(defaultDoc, pageOverride)
	if err != nil {
		log.Printf("default-config: ignoring invalid merged override for page=%s team=%d: %v", pageID, teamID, err)
		return defaultDoc, nil
	}
	return merged, nil
}
