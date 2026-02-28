package model

// ---- SLO / SLI ----

type SloSliResponse struct {
	Objectives Objectives  `json:"objectives"`
	Status     SloStatus   `json:"status"`
	Summary    SloSummary  `json:"summary"`
	Timeseries []SloBucket `json:"timeseries"`
}

type Objectives struct {
	AvailabilityTarget float64 `json:"availabilityTarget"`
	P95LatencyTargetMs float64 `json:"p95LatencyTargetMs"`
}

type SloStatus struct {
	AvailabilityPercent         float64 `json:"availabilityPercent"`
	P95LatencyMs                float64 `json:"p95LatencyMs"`
	ErrorBudgetRemainingPercent float64 `json:"errorBudgetRemainingPercent"`
	Compliant                   bool    `json:"compliant"`
}

type SloSummary struct {
	TotalRequests       int64   `json:"total_requests"`
	ErrorCount          int64   `json:"error_count"`
	AvailabilityPercent float64 `json:"availability_percent"`
	AvgLatencyMs        float64 `json:"avg_latency_ms"`
	P95LatencyMs        float64 `json:"p95_latency_ms"`
}

type SloBucket struct {
	Timestamp           string   `json:"timestamp"`
	RequestCount        int64    `json:"request_count"`
	ErrorCount          int64    `json:"error_count"`
	AvailabilityPercent float64  `json:"availability_percent"`
	AvgLatencyMs        *float64 `json:"avg_latency_ms"`
}

// ---- Logs Stream ----

type LogsStreamResponse struct {
	Stream           []LogStreamItem   `json:"stream"`
	Total            int64             `json:"total"`
	VolumeTrends     []LogVolumeBucket `json:"volumeTrends"`
	TraceCorrelation TraceCorrelation  `json:"traceCorrelation"`
	Facets           LogFacets         `json:"facets"`
}

type LogStreamItem struct {
	Timestamp   string `json:"timestamp"`
	Level       string `json:"level"`
	ServiceName string `json:"service_name"`
	Logger      string `json:"logger"`
	Message     string `json:"message"`
	TraceID     string `json:"trace_id"`
	SpanID      string `json:"span_id"`
	Host        string `json:"host"`
	Pod         string `json:"pod"`
	Container   string `json:"container"`
	Thread      string `json:"thread"`
	Exception   string `json:"exception"`
}

type LogVolumeBucket struct {
	Timestamp          string `json:"timestamp"`
	LogCount           int64  `json:"log_count"`
	CorrelatedLogCount int64  `json:"correlated_log_count"`
}

type TraceCorrelation struct {
	TraceCorrelatedLogs int64   `json:"traceCorrelatedLogs"`
	UncorrelatedLogs    int64   `json:"uncorrelatedLogs"`
	CorrelationRatio    float64 `json:"correlationRatio"`
}

type LogFacets struct {
	Levels   []Facet `json:"levels"`
	Services []Facet `json:"services"`
}

type Facet struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

// End of Insights Models
