package dashboardcfg

import (
	queryvalue "github.com/Optikk-Org/optikk-backend/internal/shared/contracts/queryvalue"
)

type RenderMode string

// CurrentSchemaVersion is the canonical dashboard page schema. Version 2 adds explicit
// panel layout grid dimensions (layout.w, layout.h) alongside x/y; see CODEBASE_INDEX.md.
const CurrentSchemaVersion = 2

// MinSupportedSchemaVersion is the oldest schemaVersion still accepted at load time (hydration may apply).
const MinSupportedSchemaVersion = 1

const (
	RenderModeDashboard RenderMode = "dashboard"
	RenderModeExplorer  RenderMode = "explorer"
)

type PanelType string
type LayoutVariant string
type SectionTemplate string

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

// PageMetadata describes a top-level page exposed to the frontend.
type PageMetadata struct {
	SchemaVersion int        `json:"schemaVersion"`
	ID            string     `json:"id"`
	Path          string     `json:"path"`
	Label         string     `json:"label"`
	Icon          string     `json:"icon"`
	Group         string     `json:"group"`
	Order         int        `json:"order"`
	DefaultTabID  string     `json:"defaultTabId,omitempty"`
	Navigable     bool       `json:"navigable"`
	RenderMode    RenderMode `json:"renderMode"`
	Title         string     `json:"title,omitempty"`
	Subtitle      string     `json:"subtitle,omitempty"`
}

// QuerySpec describes the backend API contract for a single panel.
type QuerySpec struct {
	Method   string                      `json:"method"`
	Endpoint string                      `json:"endpoint"`
	Params   map[string]queryvalue.Value `json:"params,omitempty"`
}

type PanelLayout struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

type SectionDefinition struct {
	ID              string          `json:"id"`
	Title           string          `json:"title"`
	Order           int             `json:"order"`
	Collapsible     bool            `json:"collapsible"`
	SectionTemplate SectionTemplate `json:"sectionTemplate"`
}

type StatSummaryField struct {
	Label string   `json:"label"`
	Field string   `json:"field,omitempty"`
	Keys  []string `json:"keys,omitempty"`
}

// PanelDefinition describes a renderable dashboard panel and its query contract.
type PanelDefinition struct {
	ID                    string             `json:"id"`
	PanelType             PanelType          `json:"panelType"`
	LayoutVariant         LayoutVariant      `json:"layoutVariant"`
	Title                 string             `json:"title,omitempty"`
	Description           string             `json:"description,omitempty"`
	TitleIcon             string             `json:"titleIcon,omitempty"`
	Icon                  string             `json:"icon,omitempty"`
	SectionID             string             `json:"sectionId"`
	Order                 int                `json:"order"`
	Query                 QuerySpec          `json:"query"`
	Layout                PanelLayout        `json:"layout"`
	DataSource            string             `json:"dataSource,omitempty"`
	DataKey               string             `json:"dataKey,omitempty"`
	GroupByKey            string             `json:"groupByKey,omitempty"`
	LabelKey              string             `json:"labelKey,omitempty"`
	XKey                  string             `json:"xKey,omitempty"`
	YKey                  string             `json:"yKey,omitempty"`
	EndpointDataSource    string             `json:"endpointDataSource,omitempty"`
	EndpointMetricsSource string             `json:"endpointMetricsSource,omitempty"`
	EndpointListType      string             `json:"endpointListType,omitempty"`
	ValueField            string             `json:"valueField,omitempty"`
	ValueKey              string             `json:"valueKey,omitempty"`
	ValueKeys             []string           `json:"valueKeys,omitempty"`
	BucketKey             string             `json:"bucketKey,omitempty"`
	DatasetLabel          string             `json:"datasetLabel,omitempty"`
	Color                 string             `json:"color,omitempty"`
	Formatter             string             `json:"formatter,omitempty"`
	Stacked               bool               `json:"stacked,omitempty"`
	ListSortField         string             `json:"listSortField,omitempty"`
	ListType              string             `json:"listType,omitempty"`
	ListTitle             string             `json:"listTitle,omitempty"`
	DrilldownRoute        string             `json:"drilldownRoute,omitempty"`
	TargetThreshold       *float64           `json:"targetThreshold,omitempty"`
	SummaryFields         []StatSummaryField `json:"summaryFields,omitempty"`
	YPrefix               string             `json:"yPrefix,omitempty"`
	YDecimals             *int               `json:"yDecimals,omitempty"`
}

// TabDefinition describes a single tab and its ordered sections and panels.
type TabDefinition struct {
	ID       string              `json:"id"`
	PageID   string              `json:"pageId"`
	Label    string              `json:"label"`
	Order    int                 `json:"order"`
	Sections []SectionDefinition `json:"sections,omitempty"`
	Panels   []PanelDefinition   `json:"panels,omitempty"`
}

// PageDocument is the canonical stored form for a page override.
type PageDocument struct {
	Page PageMetadata    `json:"page"`
	Tabs []TabDefinition `json:"tabs,omitempty"`
}
