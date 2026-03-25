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

func (s *Service) SavePageOverride(teamID int64, pageID string, override configdefaults.PageDocument) error {
	defaultDoc, ok := s.registry.GetPage(pageID)
	if !ok {
		return httpError("No configuration found for page: " + pageID)
	}

	if err := normalizeOverridePageDocument(&override, defaultDoc); err != nil {
		return err
	}
	if err := configdefaults.ValidatePageDocument(override); err != nil {
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
		s.seedDefaultDoc(teamID, pageID, defaultDoc)
		return defaultDoc, nil
	}

	pageOverride, err := configdefaults.DecodePageDocument([]byte(override.ConfigJSON))
	if err != nil {
		log.Printf("default-config: ignoring invalid override for page=%s team=%d: %v", pageID, teamID, err)
		s.seedDefaultDoc(teamID, pageID, defaultDoc)
		return defaultDoc, nil
	}

	if err := normalizeOverridePageDocument(&pageOverride, defaultDoc); err != nil {
		log.Printf("default-config: ignoring invalid override metadata for page=%s team=%d: %v", pageID, teamID, err)
		s.seedDefaultDoc(teamID, pageID, defaultDoc)
		return defaultDoc, nil
	}

	if err := configdefaults.ValidatePageDocument(pageOverride); err != nil {
		log.Printf("default-config: ignoring invalid override document for page=%s team=%d: %v", pageID, teamID, err)
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
		log.Printf("default-config: failed to seed page=%s for team=%d: %v", pageID, teamID, saveErr)
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
