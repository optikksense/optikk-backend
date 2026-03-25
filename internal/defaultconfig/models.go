package defaultconfig

import (
	queryvalue "github.com/observability/observability-backend-go/internal/contracts/queryvalue"
)

type RenderMode string

const CurrentSchemaVersion = 1

const (
	RenderModeDashboard RenderMode = "dashboard"
	RenderModeExplorer  RenderMode = "explorer"
)

type SectionKind string

const (
	SectionKindSummary    SectionKind = "summary"
	SectionKindTrends     SectionKind = "trends"
	SectionKindBreakdowns SectionKind = "breakdowns"
	SectionKindDetails    SectionKind = "details"
)

type SectionLayoutMode string

const (
	SectionLayoutModeKPIStrip SectionLayoutMode = "kpi-strip"
	SectionLayoutModeTwoUp    SectionLayoutMode = "two-up"
	SectionLayoutModeThreeUp  SectionLayoutMode = "three-up"
	SectionLayoutModeStack    SectionLayoutMode = "stack"
)

type PanelPreset string

const (
	PanelPresetKPI       PanelPreset = "kpi"
	PanelPresetTrend     PanelPreset = "trend"
	PanelPresetHero      PanelPreset = "hero"
	PanelPresetBreakdown PanelPreset = "breakdown"
	PanelPresetDetail    PanelPreset = "detail"
)

type PanelType string

const (
	PanelTypeAIBar             PanelType = "ai-bar"
	PanelTypeAILine            PanelType = "ai-line"
	PanelTypeBar               PanelType = "bar"
	PanelTypeDBSystemsOverview PanelType = "db-systems-overview"
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
	PanelTypeScorecard         PanelType = "scorecard"
	PanelTypeServiceHealthGrid PanelType = "service-health-grid"
	PanelTypeServiceMap        PanelType = "service-map"
	PanelTypeSLOIndicators     PanelType = "slo-indicators"
	PanelTypeStatCard          PanelType = "stat-card"
	PanelTypeStatCardsGrid     PanelType = "stat-cards-grid"
	PanelTypeStatSummary       PanelType = "stat-summary"
	PanelTypeTable             PanelType = "table"
)

var allowedPanelTypes = map[PanelType]struct{}{
	PanelTypeAIBar:             {},
	PanelTypeAILine:            {},
	PanelTypeBar:               {},
	PanelTypeDBSystemsOverview: {},
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
	PanelTypeScorecard:         {},
	PanelTypeServiceHealthGrid: {},
	PanelTypeServiceMap:        {},
	PanelTypeSLOIndicators:     {},
	PanelTypeStatCard:          {},
	PanelTypeStatCardsGrid:     {},
	PanelTypeStatSummary:       {},
	PanelTypeTable:             {},
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
	Preset PanelPreset `json:"preset"`
	X      *int        `json:"x,omitempty"`
	Y      *int        `json:"y,omitempty"`
	W      *int        `json:"w,omitempty"`
	H      *int        `json:"h,omitempty"`
}

type SectionDefinition struct {
	ID          string            `json:"id"`
	Title       string            `json:"title"`
	Order       int               `json:"order"`
	Kind        SectionKind       `json:"kind"`
	LayoutMode  SectionLayoutMode `json:"layoutMode"`
	Collapsible bool              `json:"collapsible"`
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
	Title                 string             `json:"title,omitempty"`
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
