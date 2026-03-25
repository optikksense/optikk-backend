package defaultconfig

import (
	"strings"
	"testing"
	"testing/fstest"

	configdefaults "github.com/observability/observability-backend-go/internal/defaultconfig"
)

type stubRepository struct {
	override   pageOverrideDTO
	saveCalls  int
	savedPages map[string]string
}

func (r *stubRepository) GetPageOverride(teamID int64, pageID string) (pageOverrideDTO, error) {
	return r.override, nil
}

func (r *stubRepository) SavePageOverride(teamID int64, pageID, configJSON string) error {
	r.saveCalls++
	if r.savedPages == nil {
		r.savedPages = map[string]string{}
	}
	r.savedPages[pageID] = configJSON
	return nil
}

func loadTestRegistry(t *testing.T) *configdefaults.Registry {
	t.Helper()

	registry, err := configdefaults.LoadFromFS(fstest.MapFS{
		"pages/overview/page.json": &fstest.MapFile{Data: []byte(`{
      "schemaVersion":1,
      "id":"overview",
      "path":"/overview",
      "label":"Overview",
      "icon":"LayoutDashboard",
      "group":"observe",
      "order":10,
      "defaultTabId":"summary",
      "navigable":true,
      "renderMode":"dashboard"
    }`)},
		"pages/overview/tabs/summary.json": &fstest.MapFile{Data: []byte(`{
      "id":"summary",
      "pageId":"overview",
      "label":"Summary",
      "order":10,
      "sections":[
        {"id":"summary","title":"Key Metrics","order":10,"kind":"summary","layoutMode":"kpi-strip","collapsible":true}
      ],
      "panels":[
        {
          "id":"requests-total",
          "panelType":"stat-card",
          "sectionId":"summary",
          "order":10,
          "layout":{"preset":"kpi"},
          "title":"Requests",
          "valueField":"request_count",
          "query":{"method":"GET","endpoint":"/v1/overview/requests"}
        }
      ]
    }`)},
	})
	if err != nil {
		t.Fatalf("LoadFromFS() error = %v", err)
	}
	return registry
}

func TestGetTabDocumentReseedsInvalidOverrideWithDefaults(t *testing.T) {
	t.Parallel()

	repo := &stubRepository{
		override: pageOverrideDTO{ConfigJSON: `{"page":{"id":"overview"},"tabs":[{"id":"summary","components":[{"id":"legacy","componentKey":"request"}]}]}`},
	}
	service := NewService(repo, loadTestRegistry(t), false)

	tab, err := service.GetTabDocument(42, "overview", "summary")
	if err != nil {
		t.Fatalf("GetTabDocument() error = %v", err)
	}

	if tab.ID != "summary" || len(tab.Panels) != 1 {
		t.Fatalf("GetTabDocument() returned unexpected tab: %+v", tab)
	}
	if repo.saveCalls != 1 {
		t.Fatalf("SavePageOverride() calls = %d, want 1", repo.saveCalls)
	}
	if !strings.Contains(repo.savedPages["overview"], `"renderMode":"dashboard"`) {
		t.Fatalf("saved override did not reseed canonical default: %s", repo.savedPages["overview"])
	}
}

func TestGetTabDocumentReseedsOverrideWithUnknownPanelField(t *testing.T) {
	t.Parallel()

	repo := &stubRepository{
		override: pageOverrideDTO{ConfigJSON: `{
      "page":{
        "schemaVersion":1,
        "id":"overview",
        "path":"/overview",
        "label":"Overview",
        "icon":"LayoutDashboard",
        "group":"observe",
        "order":10,
        "defaultTabId":"summary",
        "navigable":true,
        "renderMode":"dashboard"
      },
      "tabs":[{
        "id":"summary",
        "pageId":"overview",
        "label":"Summary",
        "order":10,
        "sections":[
          {"id":"summary","title":"Key Metrics","order":10,"kind":"summary","layoutMode":"kpi-strip","collapsible":true}
        ],
        "panels":[{
          "id":"requests-total",
          "panelType":"stat-card",
          "sectionId":"summary",
          "order":10,
          "layout":{"preset":"kpi"},
          "title":"Requests",
          "valueField":"request_count",
          "query":{"method":"GET","endpoint":"/v1/overview/requests"},
          "unexpected":"nope"
        }]
      }]
    }`},
	}
	service := NewService(repo, loadTestRegistry(t), false)

	tab, err := service.GetTabDocument(42, "overview", "summary")
	if err != nil {
		t.Fatalf("GetTabDocument() error = %v", err)
	}

	if tab.ID != "summary" || len(tab.Panels) != 1 {
		t.Fatalf("GetTabDocument() returned unexpected tab: %+v", tab)
	}
	if repo.saveCalls != 1 {
		t.Fatalf("SavePageOverride() calls = %d, want 1", repo.saveCalls)
	}
	if !strings.Contains(repo.savedPages["overview"], `"panelType":"stat-card"`) {
		t.Fatalf("saved override did not reseed canonical default: %s", repo.savedPages["overview"])
	}
}
