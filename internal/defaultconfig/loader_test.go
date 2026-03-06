package defaultconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFS_SortsTabsAndComponents(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "pages/overview/page.json"), `{
  "id": "overview",
  "path": "/overview",
  "label": "Overview",
  "icon": "LayoutDashboard",
  "group": "observe",
  "order": 10,
  "defaultTabId": "summary",
  "navigable": true
}`)
	mustWriteFile(t, filepath.Join(root, "pages/overview/tabs/slow.json"), `{
  "id": "slow",
  "pageId": "overview",
  "label": "Slow",
  "order": 20,
  "components": [
    {
      "id": "b",
      "componentKey": "table",
      "order": 20,
      "query": { "method": "GET", "endpoint": "/v1/b" }
    },
    {
      "id": "a",
      "componentKey": "table",
      "order": 10,
      "query": { "method": "GET", "endpoint": "/v1/a" }
    }
  ]
}`)
	mustWriteFile(t, filepath.Join(root, "pages/overview/tabs/summary.json"), `{
  "id": "summary",
  "pageId": "overview",
  "label": "Summary",
  "order": 10,
  "components": [
    {
      "id": "request-rate",
      "componentKey": "request",
      "order": 10,
      "query": { "method": "GET", "endpoint": "/v1/request-rate" }
    }
  ]
}`)

	registry, err := LoadFromFS(os.DirFS(root))
	if err != nil {
		t.Fatalf("LoadFromFS returned error: %v", err)
	}

	doc, ok := registry.GetPage("overview")
	if !ok {
		t.Fatalf("expected overview page to exist")
	}
	if len(doc.Tabs) != 2 {
		t.Fatalf("expected 2 tabs, got %d", len(doc.Tabs))
	}
	if doc.Tabs[0].ID != "summary" {
		t.Fatalf("expected summary tab first, got %s", doc.Tabs[0].ID)
	}
	if doc.Tabs[1].Components[0].ID != "a" {
		t.Fatalf("expected components to be sorted by order")
	}
}

func TestLoadFromFS_RejectsMissingDefaultTab(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "pages/overview/page.json"), `{
  "id": "overview",
  "path": "/overview",
  "label": "Overview",
  "icon": "LayoutDashboard",
  "group": "observe",
  "order": 10,
  "defaultTabId": "summary",
  "navigable": true
}`)
	mustWriteFile(t, filepath.Join(root, "pages/overview/tabs/default.json"), `{
  "id": "default",
  "pageId": "overview",
  "label": "Default",
  "order": 10,
  "components": []
}`)

	_, err := LoadFromFS(os.DirFS(root))
	if err == nil {
		t.Fatalf("expected missing default tab validation error")
	}
}

func TestMergePageDocument_ReplacesSpecifiedTabs(t *testing.T) {
	base := PageDocument{
		Page: PageMetadata{
			ID:           "overview",
			Path:         "/overview",
			Label:        "Overview",
			Icon:         "LayoutDashboard",
			Group:        "observe",
			Order:        10,
			DefaultTabID: "summary",
			Navigable:    true,
		},
		Tabs: []TabDefinition{
			{
				ID:     "summary",
				PageID: "overview",
				Label:  "Summary",
				Order:  10,
				Components: []Component{
					{
						ID:           "request-rate",
						ComponentKey: "request",
						Order:        10,
						Query: QuerySpec{
							Method:   "GET",
							Endpoint: "/v1/request-rate",
						},
					},
				},
			},
		},
	}

	override := PageDocument{
		Page: PageMetadata{
			ID:           "overview",
			Path:         "/custom-overview",
			Label:        "Custom Overview",
			Icon:         "LayoutDashboard",
			Group:        "observe",
			Order:        5,
			DefaultTabID: "summary",
			Navigable:    true,
		},
		Tabs: []TabDefinition{
			{
				ID:     "summary",
				PageID: "overview",
				Label:  "Summary",
				Order:  10,
				Components: []Component{
					{
						ID:           "latency",
						ComponentKey: "latency",
						Order:        10,
						Query: QuerySpec{
							Method:   "GET",
							Endpoint: "/v1/latency",
						},
					},
				},
			},
		},
	}

	merged, err := MergePageDocument(base, override)
	if err != nil {
		t.Fatalf("MergePageDocument returned error: %v", err)
	}
	if merged.Page.Path != "/custom-overview" {
		t.Fatalf("expected page metadata override to win")
	}
	if len(merged.Tabs) != 1 || merged.Tabs[0].Components[0].ID != "latency" {
		t.Fatalf("expected override tab components to replace defaults")
	}
}

func mustWriteFile(t *testing.T, filePath, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filePath, err)
	}
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", filePath, err)
	}
}
