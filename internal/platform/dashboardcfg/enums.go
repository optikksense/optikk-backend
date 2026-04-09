package dashboardcfg

const (
	RenderModeDashboard RenderMode = "dashboard"
	RenderModeExplorer  RenderMode = "explorer"
)

const (
	PanelTypeAIBar             PanelType = "ai-bar"
	PanelTypeAILine            PanelType = "ai-line"
	PanelTypeBar               PanelType = "bar"
	PanelTypeDBSystemsOverview PanelType = "db-systems-overview"
	PanelTypeErrorHotspotRank  PanelType = "error-hotspot-ranking"
	PanelTypeErrorRate         PanelType = "error-rate"
	PanelTypeExceptionTypeLine PanelType = "exception-type-line"
	PanelTypeGauge             PanelType = "gauge"
	PanelTypeHeatmap           PanelType = "heatmap"
	PanelTypeLatency           PanelType = "latency"
	PanelTypeLatencyHeatmap    PanelType = "latency-heatmap"
	PanelTypeLatencyHistogram  PanelType = "latency-histogram"
	PanelTypeLogHistogram      PanelType = "log-histogram"
	PanelTypePie               PanelType = "pie"
	PanelTypeRequest           PanelType = "request"
	PanelTypeServiceCatalog    PanelType = "service-catalog"
	PanelTypeServiceHealthGrid PanelType = "service-health-grid"
	PanelTypeServiceMap        PanelType = "service-map"
	PanelTypeSLOIndicators     PanelType = "slo-indicators"
	PanelTypeStatCard          PanelType = "stat-card"
	PanelTypeStatCardsGrid     PanelType = "stat-cards-grid"
	PanelTypeStatSummary       PanelType = "stat-summary"
	PanelTypeTable             PanelType = "table"
	PanelTypeTraceWaterfall    PanelType = "trace-waterfall"
)

const (
	LayoutVariantKPI           LayoutVariant = "kpi"
	LayoutVariantSummary       LayoutVariant = "summary"
	LayoutVariantStandardChart LayoutVariant = "standard-chart"
	LayoutVariantWideChart     LayoutVariant = "wide-chart"
	LayoutVariantRanking       LayoutVariant = "ranking"
	LayoutVariantSummaryTable  LayoutVariant = "summary-table"
	LayoutVariantDetailTable   LayoutVariant = "detail-table"
	LayoutVariantHero          LayoutVariant = "hero"
	LayoutVariantHeroMap       LayoutVariant = "hero-map"
	LayoutVariantHeroDetail    LayoutVariant = "hero-detail"
	LayoutVariantCompact       LayoutVariant = "compact"
	LayoutVariantWideCompact   LayoutVariant = "wide-compact"
)

const (
	SectionTemplateKPIBand              SectionTemplate = "kpi-band"
	SectionTemplateSummaryPlusHealth    SectionTemplate = "summary-plus-health"
	SectionTemplateTwoUp                SectionTemplate = "two-up"
	SectionTemplateThreeUp              SectionTemplate = "three-up"
	SectionTemplateStacked              SectionTemplate = "stacked"
	SectionTemplateHeroPlusTable        SectionTemplate = "hero-plus-table"
	SectionTemplateChartGridPlusDetails SectionTemplate = "chart-grid-plus-details"
	SectionTemplateTableStack           SectionTemplate = "table-stack"
)

const (
	ColumnAlignLeft   ColumnAlign = "left"
	ColumnAlignCenter ColumnAlign = "center"
	ColumnAlignRight  ColumnAlign = "right"
)

const (
	DrawerEntityAIModel        DrawerEntity = "aiModel"
	DrawerEntityDatabaseSystem DrawerEntity = "databaseSystem"
	DrawerEntityErrorGroup     DrawerEntity = "errorGroup"
	DrawerEntityKafkaGroup     DrawerEntity = "kafkaGroup"
	DrawerEntityKafkaTopic     DrawerEntity = "kafkaTopic"
	DrawerEntityNode           DrawerEntity = "node"
	DrawerEntityRedisInstance  DrawerEntity = "redisInstance"
	DrawerEntityService        DrawerEntity = "service"
)

var allowedPanelTypes = map[PanelType]struct{}{
	PanelTypeAIBar:             {},
	PanelTypeAILine:            {},
	PanelTypeBar:               {},
	PanelTypeDBSystemsOverview: {},
	PanelTypeErrorHotspotRank:  {},
	PanelTypeErrorRate:         {},
	PanelTypeExceptionTypeLine: {},
	PanelTypeGauge:             {},
	PanelTypeHeatmap:           {},
	PanelTypeLatency:           {},
	PanelTypeLatencyHeatmap:    {},
	PanelTypeLatencyHistogram:  {},
	PanelTypeLogHistogram:      {},
	PanelTypePie:               {},
	PanelTypeRequest:           {},
	PanelTypeServiceCatalog:    {},
	PanelTypeServiceHealthGrid: {},
	PanelTypeServiceMap:        {},
	PanelTypeSLOIndicators:     {},
	PanelTypeStatCard:          {},
	PanelTypeStatCardsGrid:     {},
	PanelTypeStatSummary:       {},
	PanelTypeTable:             {},
	PanelTypeTraceWaterfall:    {},
}

var allowedLayoutVariants = map[LayoutVariant]struct{}{
	LayoutVariantKPI:           {},
	LayoutVariantSummary:       {},
	LayoutVariantStandardChart: {},
	LayoutVariantWideChart:     {},
	LayoutVariantRanking:       {},
	LayoutVariantSummaryTable:  {},
	LayoutVariantDetailTable:   {},
	LayoutVariantHero:          {},
	LayoutVariantHeroMap:       {},
	LayoutVariantHeroDetail:    {},
	LayoutVariantCompact:       {},
	LayoutVariantWideCompact:   {},
}

var allowedSectionTemplates = map[SectionTemplate]struct{}{
	SectionTemplateKPIBand:              {},
	SectionTemplateSummaryPlusHealth:    {},
	SectionTemplateTwoUp:                {},
	SectionTemplateThreeUp:              {},
	SectionTemplateStacked:              {},
	SectionTemplateHeroPlusTable:        {},
	SectionTemplateChartGridPlusDetails: {},
	SectionTemplateTableStack:           {},
}

var allowedColumnAligns = map[ColumnAlign]struct{}{
	ColumnAlignLeft:   {},
	ColumnAlignCenter: {},
	ColumnAlignRight:  {},
}

var allowedDrawerEntities = map[DrawerEntity]struct{}{
	DrawerEntityAIModel:        {},
	DrawerEntityDatabaseSystem: {},
	DrawerEntityErrorGroup:     {},
	DrawerEntityKafkaGroup:     {},
	DrawerEntityKafkaTopic:     {},
	DrawerEntityNode:           {},
	DrawerEntityRedisInstance:  {},
	DrawerEntityService:        {},
}

var allowedFormatters = map[string]struct{}{
	"ms":       {},
	"ns":       {},
	"bytes":    {},
	"percent1": {},
	"percent2": {},
	"number":   {},
}
