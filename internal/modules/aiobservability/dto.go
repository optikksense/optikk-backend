package aiobservability

const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000

type QueryOptions struct {
	TeamID  int64
	StartMs int64
	EndMs   int64
	Limit   int
	Filters Filters
}

type Filters struct {
	Providers      []string
	Models         []string
	Operations     []string
	Services       []string
	Environments   []string
	PromptNames    []string
	PromptVersions []string
	AgentNames     []string
	ToolNames      []string
	DataSources    []string
}

type ModelRatePoint struct {
	Timestamp string  `json:"timestamp"`
	Provider  string  `json:"provider"`
	Model     string  `json:"model"`
	Operation string  `json:"operation"`
	Requests  uint64  `json:"requests"`
	Rate      float64 `json:"rate"`
}

type ModelLatencyPoint struct {
	Timestamp string  `json:"timestamp"`
	Provider  string  `json:"provider"`
	Model     string  `json:"model"`
	Operation string  `json:"operation"`
	AvgMs     float64 `json:"avgMs"`
	P50Ms     float64 `json:"p50Ms"`
	P95Ms     float64 `json:"p95Ms"`
	P99Ms     float64 `json:"p99Ms"`
}

type ModelErrorRatePoint struct {
	Timestamp string  `json:"timestamp"`
	Provider  string  `json:"provider"`
	Model     string  `json:"model"`
	Operation string  `json:"operation"`
	Requests  uint64  `json:"requests"`
	Errors    uint64  `json:"errors"`
	ErrorRate float64 `json:"errorRate"`
}

type TokenUsagePoint struct {
	Timestamp    string `json:"timestamp"`
	Provider     string `json:"provider,omitempty"`
	Model        string `json:"model,omitempty"`
	Operation    string `json:"operation,omitempty"`
	InputTokens  uint64 `json:"inputTokens"`
	OutputTokens uint64 `json:"outputTokens"`
	TotalTokens  uint64 `json:"totalTokens"`
}

type CostPoint struct {
	Timestamp           string  `json:"timestamp"`
	Provider            string  `json:"provider,omitempty"`
	Model               string  `json:"model,omitempty"`
	Operation           string  `json:"operation,omitempty"`
	EstimatedInputCost  float64 `json:"estimatedInputCost"`
	EstimatedOutputCost float64 `json:"estimatedOutputCost"`
	EstimatedTotalCost  float64 `json:"estimatedTotalCost"`
}

type CallRow struct {
	Timestamp          string  `json:"timestamp"`
	TraceID            string  `json:"traceId"`
	SpanID             string  `json:"spanId"`
	Service            string  `json:"service"`
	Environment        string  `json:"environment"`
	Provider           string  `json:"provider"`
	Model              string  `json:"model"`
	Operation          string  `json:"operation"`
	Name               string  `json:"name"`
	DurationMs         float64 `json:"durationMs"`
	InputTokens        uint64  `json:"inputTokens"`
	OutputTokens       uint64  `json:"outputTokens"`
	TotalTokens        uint64  `json:"totalTokens"`
	EstimatedTotalCost float64 `json:"estimatedTotalCost"`
	Status             string  `json:"status"`
	ErrorType          string  `json:"errorType,omitempty"`
}

type PromptUsagePoint struct {
	Timestamp     string `json:"timestamp"`
	PromptName    string `json:"promptName"`
	PromptVersion string `json:"promptVersion,omitempty"`
	Calls         uint64 `json:"calls"`
}

type PromptLatencyPoint struct {
	Timestamp     string  `json:"timestamp"`
	PromptName    string  `json:"promptName"`
	PromptVersion string  `json:"promptVersion"`
	AvgMs         float64 `json:"avgMs"`
	P50Ms         float64 `json:"p50Ms"`
	P95Ms         float64 `json:"p95Ms"`
	P99Ms         float64 `json:"p99Ms"`
}

type PromptTokenUsagePoint struct {
	Timestamp     string `json:"timestamp"`
	PromptName    string `json:"promptName"`
	PromptVersion string `json:"promptVersion"`
	InputTokens   uint64 `json:"inputTokens"`
	OutputTokens  uint64 `json:"outputTokens"`
	TotalTokens   uint64 `json:"totalTokens"`
}

type PromptCostPoint struct {
	Timestamp           string  `json:"timestamp"`
	PromptName          string  `json:"promptName"`
	PromptVersion       string  `json:"promptVersion"`
	EstimatedInputCost  float64 `json:"estimatedInputCost"`
	EstimatedOutputCost float64 `json:"estimatedOutputCost"`
	EstimatedTotalCost  float64 `json:"estimatedTotalCost"`
}

type TraceRow struct {
	Timestamp          string  `json:"timestamp"`
	TraceID            string  `json:"traceId"`
	SpanID             string  `json:"spanId"`
	Service            string  `json:"service"`
	Environment        string  `json:"environment"`
	Provider           string  `json:"provider"`
	Model              string  `json:"model"`
	Operation          string  `json:"operation"`
	Name               string  `json:"name"`
	PromptName         string  `json:"promptName,omitempty"`
	PromptVersion      string  `json:"promptVersion,omitempty"`
	AgentName          string  `json:"agentName,omitempty"`
	ToolName           string  `json:"toolName,omitempty"`
	DataSource         string  `json:"dataSource,omitempty"`
	DurationMs         float64 `json:"durationMs"`
	TotalTokens        uint64  `json:"totalTokens"`
	EstimatedTotalCost float64 `json:"estimatedTotalCost"`
	Status             string  `json:"status"`
	ErrorType          string  `json:"errorType,omitempty"`
}

type AgentRunPoint struct {
	Timestamp string `json:"timestamp"`
	AgentName string `json:"agentName"`
	Operation string `json:"operation"`
	Runs      uint64 `json:"runs"`
	Errors    uint64 `json:"errors"`
}

type ToolCallPoint struct {
	Timestamp string `json:"timestamp"`
	ToolName  string `json:"toolName"`
	ToolType  string `json:"toolType"`
	Calls     uint64 `json:"calls"`
}

type ToolErrorPoint struct {
	Timestamp string `json:"timestamp"`
	ToolName  string `json:"toolName"`
	ErrorType string `json:"errorType"`
	Errors    uint64 `json:"errors"`
}

type ToolLatencyPoint struct {
	Timestamp string  `json:"timestamp"`
	ToolName  string  `json:"toolName"`
	ToolType  string  `json:"toolType"`
	AvgMs     float64 `json:"avgMs"`
	P50Ms     float64 `json:"p50Ms"`
	P95Ms     float64 `json:"p95Ms"`
	P99Ms     float64 `json:"p99Ms"`
}

type RetrievalRatePoint struct {
	Timestamp  string  `json:"timestamp"`
	DataSource string  `json:"dataSource"`
	Provider   string  `json:"provider"`
	Requests   uint64  `json:"requests"`
	Rate       float64 `json:"rate"`
}

type RetrievalLatencyPoint struct {
	Timestamp  string  `json:"timestamp"`
	DataSource string  `json:"dataSource"`
	Provider   string  `json:"provider"`
	AvgMs      float64 `json:"avgMs"`
	P50Ms      float64 `json:"p50Ms"`
	P95Ms      float64 `json:"p95Ms"`
	P99Ms      float64 `json:"p99Ms"`
}

type RetrievalErrorPoint struct {
	Timestamp  string  `json:"timestamp"`
	DataSource string  `json:"dataSource"`
	Provider   string  `json:"provider"`
	Requests   uint64  `json:"requests"`
	Errors     uint64  `json:"errors"`
	ErrorRate  float64 `json:"errorRate"`
}

type Facets struct {
	Providers    []FacetBucket `json:"providers"`
	Models       []FacetBucket `json:"models"`
	Operations   []FacetBucket `json:"operations"`
	Services     []FacetBucket `json:"services"`
	Environments []FacetBucket `json:"environments"`
	PromptNames  []FacetBucket `json:"promptNames"`
	AgentNames   []FacetBucket `json:"agentNames"`
	ToolNames    []FacetBucket `json:"toolNames"`
	DataSources  []FacetBucket `json:"dataSources"`
}

type FacetBucket struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}
