package analytics

// AnalyticsQuery is the request body for POST /v1/spans/analytics.
type AnalyticsQuery struct {
	GroupBy      []string      `json:"groupBy"`
	Aggregations []Aggregation `json:"aggregations"`
	Filters      QueryFilters  `json:"filters"`
	OrderBy      string        `json:"orderBy"`
	OrderDir     string        `json:"orderDir"`
	Limit        int           `json:"limit"`
}

// QueryFilters mirrors the trace filter parameters accepted via query string.
type QueryFilters struct {
	StartMs     int64    `json:"startMs"`
	EndMs       int64    `json:"endMs"`
	Services    []string `json:"services,omitempty"`
	Status      string   `json:"status,omitempty"`
	MinDuration string   `json:"minDuration,omitempty"`
	MaxDuration string   `json:"maxDuration,omitempty"`
	Operation   string   `json:"operation,omitempty"`
	HTTPMethod  string   `json:"httpMethod,omitempty"`
	HTTPStatus  string   `json:"httpStatus,omitempty"`
	SearchMode  string   `json:"searchMode,omitempty"`
	SpanKind    string   `json:"spanKind,omitempty"`
	SpanName    string   `json:"spanName,omitempty"`
}

// Aggregation defines a single aggregation to compute.
type Aggregation struct {
	Type  string `json:"type"`            // count, avg, p50, p95, p99, min, max, sum, countIf, rate
	Field string `json:"field,omitempty"` // column to aggregate on (e.g. "duration_nano")
	Alias string `json:"alias"`           // output column name
}

type AnalyticsValueType string

const (
	AnalyticsValueString  AnalyticsValueType = "string"
	AnalyticsValueInteger AnalyticsValueType = "integer"
	AnalyticsValueNumber  AnalyticsValueType = "number"
	AnalyticsValueBoolean AnalyticsValueType = "boolean"
)

type AnalyticsCell struct {
	Key          string             `json:"key"`
	Type         AnalyticsValueType `json:"type"`
	StringValue  *string            `json:"stringValue,omitempty"`
	IntegerValue *int64             `json:"integerValue,omitempty"`
	NumberValue  *float64           `json:"numberValue,omitempty"`
	BooleanValue *bool              `json:"booleanValue,omitempty"`
}

type AnalyticsRow struct {
	Cells []AnalyticsCell `json:"cells"`
}

// AnalyticsResult wraps the analytics response.
type AnalyticsResult struct {
	Columns []string       `json:"columns"`
	Rows    []AnalyticsRow `json:"rows"`
}

// Dimension describes an available groupBy dimension.
type Dimension struct {
	Name        string `json:"name"`
	Column      string `json:"column"`
	Description string `json:"description"`
}
