package dashboardcfg

import (
	"fmt"
	"strings"
)

var sectionTemplateAllowedVariants = map[SectionTemplate]map[LayoutVariant]struct{}{
	SectionTemplateKPIBand: {
		LayoutVariantKPI:         {},
		LayoutVariantSummary:     {},
		LayoutVariantWideChart:   {},
		LayoutVariantCompact:     {},
		LayoutVariantWideCompact: {},
	},
	SectionTemplateSummaryPlusHealth: {
		LayoutVariantSummary: {},
		LayoutVariantRanking: {},
	},
	SectionTemplateTwoUp: {
		LayoutVariantStandardChart: {},
		LayoutVariantWideChart:     {},
	},
	SectionTemplateThreeUp: {
		LayoutVariantSummary: {},
		LayoutVariantRanking: {},
		LayoutVariantCompact: {},
	},
	SectionTemplateStacked: {
		LayoutVariantWideChart:   {},
		LayoutVariantDetailTable: {},
		LayoutVariantHero:        {},
		LayoutVariantHeroMap:     {},
		LayoutVariantHeroDetail:  {},
		LayoutVariantWideCompact: {},
	},
	SectionTemplateHeroPlusTable: {
		LayoutVariantHero:         {},
		LayoutVariantHeroMap:      {},
		LayoutVariantHeroDetail:   {},
		LayoutVariantSummaryTable: {},
		LayoutVariantDetailTable:  {},
	},
	SectionTemplateChartGridPlusDetails: {
		LayoutVariantSummary:       {},
		LayoutVariantStandardChart: {},
		LayoutVariantWideChart:     {},
		LayoutVariantRanking:       {},
		LayoutVariantSummaryTable:  {},
		LayoutVariantDetailTable:   {},
		LayoutVariantHeroDetail:    {},
		LayoutVariantCompact:       {},
		LayoutVariantWideCompact:   {},
	},
	SectionTemplateTableStack: {
		LayoutVariantSummaryTable: {},
		LayoutVariantDetailTable:  {},
	},
}

func canonicalSectionLayout(section SectionDefinition, panels []PanelDefinition) (map[string]PanelLayout, error) {
	if len(panels) == 0 {
		return map[string]PanelLayout{}, nil
	}

	if err := validateSectionTemplateCompatibility(section, panels); err != nil {
		return nil, err
	}

	layouts := make(map[string]PanelLayout, len(panels))
	x := 0
	y := 0
	rowHeight := 0

	for index, panel := range panels {
		size := PanelSize{W: panel.Layout.W, H: panel.Layout.H}

		if shouldStartNewRow(section.SectionTemplate, panel, index, x) {
			y += rowHeight
			x = 0
			rowHeight = 0
		}

		if x > 0 && x+size.W > gridCols {
			y += rowHeight
			x = 0
			rowHeight = 0
		}

		layouts[panel.ID] = PanelLayout{X: x, Y: y, W: size.W, H: size.H}
		x += size.W
		if size.H > rowHeight {
			rowHeight = size.H
		}
	}

	return layouts, nil
}

func validateSectionTemplateCompatibility(section SectionDefinition, panels []PanelDefinition) error {
	allowedVariants, ok := sectionTemplateAllowedVariants[section.SectionTemplate]
	if !ok {
		return fmt.Errorf("unsupported sectionTemplate %q", section.SectionTemplate)
	}

	for _, panel := range panels {
		if _, allowed := allowedVariants[panel.LayoutVariant]; !allowed {
			return fmt.Errorf(
				"panel %q layoutVariant %q is not allowed in sectionTemplate %q",
				panel.ID,
				panel.LayoutVariant,
				section.SectionTemplate,
			)
		}
	}

	if section.SectionTemplate == SectionTemplateHeroPlusTable {
		first := panels[0].LayoutVariant
		if first != LayoutVariantHero && first != LayoutVariantHeroMap && first != LayoutVariantHeroDetail {
			return fmt.Errorf("hero-plus-table section must start with a hero variant, got %q", first)
		}
		for _, panel := range panels[1:] {
			if panel.LayoutVariant != LayoutVariantSummaryTable && panel.LayoutVariant != LayoutVariantDetailTable {
				return fmt.Errorf(
					"hero-plus-table section only allows tables after the first hero panel; panel %q has %q",
					panel.ID,
					panel.LayoutVariant,
				)
			}
		}
	}

	return nil
}

func shouldStartNewRow(sectionTemplate SectionTemplate, panel PanelDefinition, index, x int) bool {
	if index == 0 {
		return false
	}

	switch sectionTemplate {
	case SectionTemplateHeroPlusTable:
		return panel.LayoutVariant == LayoutVariantSummaryTable || panel.LayoutVariant == LayoutVariantDetailTable
	case SectionTemplateStacked:
		return true
	case SectionTemplateTableStack:
		// Flow left-to-right so half-width tables (e.g. summary-table 6+6) sit on one row.
		return false
	case SectionTemplateKPIBand:
		return panel.LayoutVariant != LayoutVariantKPI
	case SectionTemplateSummaryPlusHealth:
		return panel.LayoutVariant == LayoutVariantRanking && x > 0
	default:
		return false
	}
}

func sectionLayoutPattern(section SectionDefinition, panels []PanelDefinition) string {
	parts := make([]string, 0, len(panels)+1)
	parts = append(parts, fmt.Sprintf("template=%s", section.SectionTemplate))
	for _, panel := range panels {
		parts = append(parts, fmt.Sprintf("%s/%s<%dx%d>", panel.PanelType, panel.LayoutVariant, panel.Layout.W, panel.Layout.H))
	}
	return strings.Join(parts, " -> ")
}
