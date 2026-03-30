package dashboardcfg

import (
	"fmt"
	"sort"
	"testing"
)

// validPanel returns a minimal PanelDefinition that passes validation.
func validPanel(id string) PanelDefinition {
	sz, _ := panelSizeForVariant(LayoutVariantStandardChart)
	return PanelDefinition{
		ID:            id,
		PanelType:     PanelTypeRequest,
		LayoutVariant: LayoutVariantStandardChart,
		SectionID:     "sec-1",
		Query:         QuerySpec{Endpoint: "/v1/overview/request-rate", Method: "GET"},
		Layout:        PanelLayout{X: 0, Y: 0, W: sz.W, H: sz.H},
	}
}

func validSection() SectionDefinition {
	return SectionDefinition{
		ID:              "sec-1",
		Title:           "Section",
		Order:           10,
		Collapsible:     true,
		SectionTemplate: SectionTemplateTwoUp,
	}
}

func TestValidatePanel_LayoutXY(t *testing.T) {
	sections := map[string]struct{}{"sec-1": {}}

	tests := []struct {
		name      string
		x         int
		y         int
		panelType PanelType
		wantErr   bool
	}{
		{"valid request panel origin", 0, 0, PanelTypeRequest, false},
		{"valid right-aligned request panel", 6, 0, PanelTypeRequest, false},
		{"out of bounds request panel", 7, 0, PanelTypeRequest, true},
		{"negative x invalid", -1, 0, PanelTypeRequest, true},
		{"negative y invalid", 0, -1, PanelTypeRequest, true},
		{"full width table valid", 0, 2, PanelTypeTable, false},
		{"full width table overflow invalid", 1, 2, PanelTypeTable, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := validPanel("p-xy")
			p.PanelType = tc.panelType
			switch tc.panelType {
			case PanelTypeTable:
				p.LayoutVariant = LayoutVariantDetailTable
			default:
				p.LayoutVariant = LayoutVariantStandardChart
			}
			sz, err := panelSizeForVariant(p.LayoutVariant)
			if err != nil {
				t.Fatal(err)
			}
			p.Layout.W, p.Layout.H = sz.W, sz.H
			p.Layout.X = tc.x
			p.Layout.Y = tc.y
			seenIDs := map[string]struct{}{}
			vErr := validatePanel("pg", "tab", p, seenIDs, sections)
			if tc.wantErr && vErr == nil {
				t.Errorf("expected error, got nil")
			}
			if !tc.wantErr && vErr != nil {
				t.Errorf("unexpected error: %v", vErr)
			}
		})
	}
}

func TestValidatePanel_RejectsUnsupportedFormatter(t *testing.T) {
	sections := map[string]struct{}{"sec-1": {}}
	p := validPanel("p-format")
	p.PanelType = PanelTypeStatCard
	p.LayoutVariant = LayoutVariantKPI
	sz, _ := panelSizeForVariant(LayoutVariantKPI)
	p.Layout.W, p.Layout.H = sz.W, sz.H
	p.ValueField = "value"
	p.Formatter = "bogus"

	err := validatePanel("pg", "tab", p, map[string]struct{}{}, sections)
	if err == nil {
		t.Fatal("expected unsupported formatter validation error, got nil")
	}
}

func TestValidatePanel_RejectsUndeclaredEndpointField(t *testing.T) {
	sections := map[string]struct{}{"sec-1": {}}
	p := validPanel("p-contract")
	p.Query.Endpoint = "/v1/overview/request-rate"
	p.GroupByKey = "not_a_real_field"

	err := validatePanel("pg", "tab", p, map[string]struct{}{}, sections)
	if err == nil {
		t.Fatal("expected endpoint field contract validation error, got nil")
	}
}

func TestValidateSectionPanelLayouts_DetectsOverlap(t *testing.T) {
	section := validSection()
	sz, _ := panelSizeForVariant(LayoutVariantStandardChart)
	panels := []PanelDefinition{
		validPanel("left"),
		{
			ID:            "right",
			PanelType:     PanelTypeErrorRate,
			LayoutVariant: LayoutVariantStandardChart,
			SectionID:     "sec-1",
			Query:         QuerySpec{Endpoint: "/v1/overview/error-rate", Method: "GET"},
			Layout:        PanelLayout{X: 3, Y: 0, W: sz.W, H: sz.H},
		},
	}

	err := validateSectionPanelLayouts("pg", "tab", section, panels)
	if err == nil {
		t.Fatal("expected overlap validation error, got nil")
	}
}

func TestValidateSectionPanelLayouts_AllowsSeparatedPanels(t *testing.T) {
	section := validSection()
	sz, _ := panelSizeForVariant(LayoutVariantStandardChart)
	panels := []PanelDefinition{
		validPanel("left"),
		{
			ID:            "right",
			PanelType:     PanelTypeErrorRate,
			LayoutVariant: LayoutVariantStandardChart,
			SectionID:     "sec-1",
			Query:         QuerySpec{Endpoint: "/v1/overview/error-rate", Method: "GET"},
			Layout:        PanelLayout{X: 6, Y: 0, W: sz.W, H: sz.H},
		},
	}

	if err := validateSectionPanelLayouts("pg", "tab", section, panels); err != nil {
		t.Fatalf("unexpected overlap validation error: %v", err)
	}
}

func TestValidateSectionPanelLayouts_RequiresCanonicalPacking(t *testing.T) {
	section := validSection()
	sz, _ := panelSizeForVariant(LayoutVariantStandardChart)
	panels := []PanelDefinition{
		validPanel("left"),
		{
			ID:            "right",
			PanelType:     PanelTypeErrorRate,
			LayoutVariant: LayoutVariantStandardChart,
			SectionID:     "sec-1",
			Query:         QuerySpec{Endpoint: "/v1/overview/error-rate", Method: "GET"},
			Layout:        PanelLayout{X: 0, Y: 9, W: sz.W, H: sz.H},
			Order:         20,
		},
	}

	err := validateSectionPanelLayouts("pg", "tab", section, panels)
	if err == nil {
		t.Fatal("expected canonical packing validation error, got nil")
	}
}

func TestHydrateTabPanelLayouts_FillsMissingDimensions(t *testing.T) {
	panels := []PanelDefinition{
		{
			ID:            "p1",
			PanelType:     PanelTypeStatCard,
			LayoutVariant: LayoutVariantKPI,
			SectionID:     "sec-1",
			Query:         QuerySpec{Endpoint: "/v1/x", Method: "GET"},
			Layout:        PanelLayout{X: 0, Y: 0},
		},
	}
	if err := hydrateTabPanelLayouts("tab", &panels); err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	sz, _ := panelSizeForVariant(LayoutVariantKPI)
	if panels[0].Layout.W != sz.W || panels[0].Layout.H != sz.H {
		t.Fatalf("layout got %+v, want w=%d h=%d", panels[0].Layout, sz.W, sz.H)
	}
}

func TestLoadFromFS_EmbeddedConfigs(t *testing.T) {
	reg, err := LoadFromFS(files)
	if err != nil {
		t.Fatalf("LoadFromFS failed on embedded configs: %v", err)
	}
	pages := reg.ListPages(false)
	if len(pages) == 0 {
		t.Error("expected at least one page from embedded configs")
	}
}

func TestEmbeddedConfigs_ReportSectionLayoutPatterns(t *testing.T) {
	reg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	patternCounts := map[string]int{}
	for _, page := range reg.pages {
		if page.Page.RenderMode != RenderModeDashboard {
			continue
		}
		for _, tab := range page.Tabs {
			panelsBySection := make(map[string][]PanelDefinition, len(tab.Sections))
			for _, panel := range tab.Panels {
				panelsBySection[panel.SectionID] = append(panelsBySection[panel.SectionID], panel)
			}
			for _, section := range tab.Sections {
				patternCounts[sectionLayoutPattern(section, panelsBySection[section.ID])]++
			}
		}
	}

	patterns := make([]string, 0, len(patternCounts))
	for pattern, count := range patternCounts {
		patterns = append(patterns, fmt.Sprintf("%03d %s", count, pattern))
	}
	sort.Strings(patterns)
	for _, pattern := range patterns {
		t.Log(pattern)
	}
}

func TestInfrastructureTabs_TargetedRegressionConfig(t *testing.T) {
	reg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	doc, ok := reg.GetPage("infrastructure")
	if !ok {
		t.Fatal("expected infrastructure page in embedded registry")
	}

	findPanel := func(tabID, panelID string) PanelDefinition {
		t.Helper()
		for _, tab := range doc.Tabs {
			if tab.ID != tabID {
				continue
			}
			for _, panel := range tab.Panels {
				if panel.ID == panelID {
					return panel
				}
			}
		}
		t.Fatalf("panel %s/%s not found", tabID, panelID)
		return PanelDefinition{}
	}

	jvmCPU := findPanel("jvm", "jvm-cpu-time")
	if jvmCPU.Formatter != "ns" {
		t.Fatalf("expected jvm-cpu-time formatter ns, got %q", jvmCPU.Formatter)
	}

	jvmClasses := findPanel("jvm", "jvm-classes")
	if jvmClasses.ValueField != "loaded" {
		t.Fatalf("expected jvm-classes valueField loaded, got %q", jvmClasses.ValueField)
	}

	jvmGC := findPanel("jvm", "jvm-gc-collections")
	if jvmGC.GroupByKey != "collector" {
		t.Fatalf("expected jvm-gc-collections groupByKey collector, got %q", jvmGC.GroupByKey)
	}

	jvmThreads := findPanel("jvm", "jvm-threads")
	if jvmThreads.GroupByKey != "daemon" {
		t.Fatalf("expected jvm-threads groupByKey daemon, got %q", jvmThreads.GroupByKey)
	}

	k8sPhases := findPanel("kubernetes", "k8s-pod-phases")
	if k8sPhases.GroupByKey != "phase" || k8sPhases.ValueKey != "count" {
		t.Fatalf("expected kubernetes phases panel to use phase/count, got %q/%q", k8sPhases.GroupByKey, k8sPhases.ValueKey)
	}

	nodesTable := findPanel("nodes", "nodes-table")
	if nodesTable.DrilldownRoute != "/infrastructure/nodes/{host}" {
		t.Fatalf("expected nodes-table drilldown route, got %q", nodesTable.DrilldownRoute)
	}
}
