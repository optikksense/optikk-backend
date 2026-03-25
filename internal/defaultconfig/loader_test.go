package defaultconfig

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadFromFSRejectsMissingSchemaVersion(t *testing.T) {
	t.Parallel()

	_, err := LoadFromFS(fstest.MapFS{
		"pages/overview/page.json": &fstest.MapFile{Data: []byte(`{
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
          "query":{"method":"GET","endpoint":"/v1/overview/requests"}
        }
      ]
    }`)},
	})
	if err == nil {
		t.Fatalf("LoadFromFS() error = nil, want schemaVersion validation error")
	}
	if !strings.Contains(err.Error(), "schemaVersion") {
		t.Fatalf("LoadFromFS() error = %v, want schemaVersion validation error", err)
	}
}

func TestLoadFromFSRejectsUnknownPanelFields(t *testing.T) {
	t.Parallel()

	_, err := LoadFromFS(fstest.MapFS{
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
          "query":{"method":"GET","endpoint":"/v1/overview/requests"},
          "unexpected":"nope"
        }
      ]
    }`)},
	})
	if err == nil {
		t.Fatalf("LoadFromFS() error = nil, want unknown field validation error")
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("LoadFromFS() error = %v, want unknown field validation error", err)
	}
}

func TestLoadFromFSRejectsUnsupportedPanelType(t *testing.T) {
	t.Parallel()

	_, err := LoadFromFS(fstest.MapFS{
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
          "panelType":"area",
          "sectionId":"summary",
          "order":10,
          "layout":{"preset":"kpi"},
          "title":"Requests",
          "query":{"method":"GET","endpoint":"/v1/overview/requests"}
        }
      ]
    }`)},
	})
	if err == nil {
		t.Fatalf("LoadFromFS() error = nil, want unsupported panelType validation error")
	}
	if !strings.Contains(err.Error(), "unsupported panelType") {
		t.Fatalf("LoadFromFS() error = %v, want unsupported panelType validation error", err)
	}
}

func TestLoadFromFSRejectsNonScalarQueryParamValues(t *testing.T) {
	t.Parallel()

	_, err := LoadFromFS(fstest.MapFS{
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
          "query":{
            "method":"GET",
            "endpoint":"/v1/overview/requests",
            "params":{"window":{"minutes":30}}
          }
        }
      ]
    }`)},
	})
	if err == nil {
		t.Fatalf("LoadFromFS() error = nil, want non-scalar query param validation error")
	}
	if !strings.Contains(err.Error(), "scalar or homogeneous primitive arrays") {
		t.Fatalf("LoadFromFS() error = %v, want non-scalar query param validation error", err)
	}
}
