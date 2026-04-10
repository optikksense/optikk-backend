package dashboardcfg

import "fmt"

const gridCols = 12

type PanelSize struct {
	W int
	H int
}

type LayoutVariantSizeRegistryEntry struct {
	LayoutVariant LayoutVariant
	Size          PanelSize
}

var layoutVariantSizeRegistry = []LayoutVariantSizeRegistryEntry{
	{LayoutVariant: LayoutVariantKPI, Size: PanelSize{W: 3, H: 3}},
	{LayoutVariant: LayoutVariantSummary, Size: PanelSize{W: 4, H: 7}},
	{LayoutVariant: LayoutVariantStandardChart, Size: PanelSize{W: 6, H: 9}},
	{LayoutVariant: LayoutVariantWideChart, Size: PanelSize{W: 12, H: 9}},
	{LayoutVariant: LayoutVariantRanking, Size: PanelSize{W: 6, H: 7}},
	{LayoutVariant: LayoutVariantSummaryTable, Size: PanelSize{W: 6, H: 9}},
	{LayoutVariant: LayoutVariantDetailTable, Size: PanelSize{W: 12, H: 10}},
	{LayoutVariant: LayoutVariantHero, Size: PanelSize{W: 12, H: 10}},
	{LayoutVariant: LayoutVariantHeroMap, Size: PanelSize{W: 12, H: 12}},
	{LayoutVariant: LayoutVariantHeroDetail, Size: PanelSize{W: 12, H: 10}},
	{LayoutVariant: LayoutVariantCompact, Size: PanelSize{W: 4, H: 3}},
	{LayoutVariant: LayoutVariantWideCompact, Size: PanelSize{W: 12, H: 4}},
}

var layoutVariantSizes = func() map[LayoutVariant]PanelSize {
	sizes := make(map[LayoutVariant]PanelSize, len(layoutVariantSizeRegistry))
	for _, entry := range layoutVariantSizeRegistry {
		sizes[entry.LayoutVariant] = entry.Size
	}
	return sizes
}()

var panelTypeAllowedLayoutVariants = map[PanelType][]LayoutVariant{
	PanelTypeBar:               {LayoutVariantStandardChart, LayoutVariantWideChart},
	PanelTypeDBSystemsOverview: {LayoutVariantHeroDetail, LayoutVariantSummary, LayoutVariantCompact},
	PanelTypeErrorHotspotRank:  {LayoutVariantRanking, LayoutVariantSummary, LayoutVariantCompact},
	PanelTypeErrorRate:         {LayoutVariantStandardChart, LayoutVariantWideChart},
	PanelTypeExceptionTypeLine: {LayoutVariantStandardChart, LayoutVariantWideChart},
	PanelTypeGauge:             {LayoutVariantSummary, LayoutVariantCompact},
	PanelTypeHeatmap:           {LayoutVariantStandardChart, LayoutVariantWideChart},
	PanelTypeLatency:           {LayoutVariantStandardChart, LayoutVariantWideChart},
	PanelTypeLatencyHeatmap:    {LayoutVariantStandardChart, LayoutVariantWideChart},
	PanelTypeLatencyHistogram:  {LayoutVariantStandardChart, LayoutVariantWideChart},
	PanelTypeLogHistogram:      {LayoutVariantWideChart, LayoutVariantHeroDetail},
	PanelTypePie:               {LayoutVariantSummary, LayoutVariantCompact},
	PanelTypeRequest:           {LayoutVariantStandardChart, LayoutVariantWideChart},
	PanelTypeServiceCatalog:    {LayoutVariantDetailTable},
	PanelTypeServiceHealthGrid: {LayoutVariantSummary, LayoutVariantCompact},
	PanelTypeServiceMap:        {LayoutVariantHeroMap},
	PanelTypeSLOIndicators:     {LayoutVariantSummary, LayoutVariantCompact},
	PanelTypeStatCard:          {LayoutVariantKPI},
	PanelTypeStatCardsGrid:     {LayoutVariantSummary, LayoutVariantCompact},
	PanelTypeStatSummary:       {LayoutVariantSummary, LayoutVariantWideChart, LayoutVariantCompact, LayoutVariantWideCompact},
	PanelTypeTable:             {LayoutVariantSummaryTable, LayoutVariantDetailTable},
	PanelTypeTraceWaterfall:    {LayoutVariantHeroDetail},
}

func LayoutVariantSizeRegistry() []LayoutVariantSizeRegistryEntry {
	cloned := make([]LayoutVariantSizeRegistryEntry, len(layoutVariantSizeRegistry))
	copy(cloned, layoutVariantSizeRegistry)
	return cloned
}

func panelSizeForVariant(layoutVariant LayoutVariant) (PanelSize, error) {
	size, ok := layoutVariantSizes[layoutVariant]
	if !ok {
		return PanelSize{}, fmt.Errorf("unsupported layoutVariant %q", layoutVariant)
	}
	return size, nil
}

// ValidateLayoutMatchesVariant returns an error if w/h do not match the canonical footprint
// for layoutVariant (strict policy).
func ValidateLayoutMatchesVariant(layoutVariant LayoutVariant, w, h int) error {
	want, err := panelSizeForVariant(layoutVariant)
	if err != nil {
		return err
	}
	if w != want.W || h != want.H {
		return fmt.Errorf(
			"layout w/h (%d×%d) must match layoutVariant %q footprint (%d×%d)",
			w, h, layoutVariant, want.W, want.H,
		)
	}
	return nil
}

func allowedVariantsForPanelType(panelType PanelType) []LayoutVariant {
	return panelTypeAllowedLayoutVariants[panelType]
}

func isLayoutVariantAllowed(panelType PanelType, layoutVariant LayoutVariant) bool {
	for _, candidate := range allowedVariantsForPanelType(panelType) {
		if candidate == layoutVariant {
			return true
		}
	}
	return false
}
