package aiobservability

import "time"

type modelRateRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Provider  string    `ch:"provider"`
	Model     string    `ch:"model"`
	Operation string    `ch:"operation"`
	Requests  uint64    `ch:"requests"`
	Rate      float64   `ch:"rate"`
}

type modelLatencyRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Provider  string    `ch:"provider"`
	Model     string    `ch:"model"`
	Operation string    `ch:"operation"`
	AvgMs     float64   `ch:"avg_ms"`
	P50Ms     float64   `ch:"p50_ms"`
	P95Ms     float64   `ch:"p95_ms"`
	P99Ms     float64   `ch:"p99_ms"`
}

type modelErrorRateRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Provider  string    `ch:"provider"`
	Model     string    `ch:"model"`
	Operation string    `ch:"operation"`
	Requests  uint64    `ch:"requests"`
	Errors    uint64    `ch:"errors"`
	ErrorRate float64   `ch:"error_rate"`
}

type tokenUsageRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	Provider     string    `ch:"provider"`
	Model        string    `ch:"model"`
	Operation    string    `ch:"operation"`
	InputTokens  float64   `ch:"input_tokens"`
	OutputTokens float64   `ch:"output_tokens"`
	TotalTokens  float64   `ch:"total_tokens"`
}

type costRow struct {
	Timestamp           time.Time `ch:"timestamp"`
	Provider            string    `ch:"provider"`
	Model               string    `ch:"model"`
	Operation           string    `ch:"operation"`
	EstimatedInputCost  float64   `ch:"estimated_input_cost"`
	EstimatedOutputCost float64   `ch:"estimated_output_cost"`
	EstimatedTotalCost  float64   `ch:"estimated_total_cost"`
}

type callScanRow struct {
	Timestamp          time.Time `ch:"timestamp"`
	TraceID            string    `ch:"trace_id"`
	SpanID             string    `ch:"span_id"`
	Service            string    `ch:"service"`
	Environment        string    `ch:"environment"`
	Provider           string    `ch:"provider"`
	Model              string    `ch:"model"`
	Operation          string    `ch:"operation"`
	Name               string    `ch:"name"`
	DurationMs         float64   `ch:"duration_ms"`
	InputTokens        uint64    `ch:"input_tokens"`
	OutputTokens       uint64    `ch:"output_tokens"`
	TotalTokens        uint64    `ch:"total_tokens"`
	EstimatedTotalCost float64   `ch:"estimated_total_cost"`
	Status             string    `ch:"status"`
	ErrorType          string    `ch:"error_type"`
}

type promptUsageRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	PromptName    string    `ch:"prompt_name"`
	PromptVersion string    `ch:"prompt_version"`
	Calls         uint64    `ch:"calls"`
}

type promptLatencyRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	PromptName    string    `ch:"prompt_name"`
	PromptVersion string    `ch:"prompt_version"`
	AvgMs         float64   `ch:"avg_ms"`
	P50Ms         float64   `ch:"p50_ms"`
	P95Ms         float64   `ch:"p95_ms"`
	P99Ms         float64   `ch:"p99_ms"`
}

type promptTokenUsageRow struct {
	Timestamp     time.Time `ch:"timestamp"`
	PromptName    string    `ch:"prompt_name"`
	PromptVersion string    `ch:"prompt_version"`
	InputTokens   uint64    `ch:"input_tokens"`
	OutputTokens  uint64    `ch:"output_tokens"`
	TotalTokens   uint64    `ch:"total_tokens"`
}

type promptCostRow struct {
	Timestamp           time.Time `ch:"timestamp"`
	PromptName          string    `ch:"prompt_name"`
	PromptVersion       string    `ch:"prompt_version"`
	EstimatedInputCost  float64   `ch:"estimated_input_cost"`
	EstimatedOutputCost float64   `ch:"estimated_output_cost"`
	EstimatedTotalCost  float64   `ch:"estimated_total_cost"`
}

type traceScanRow struct {
	Timestamp          time.Time `ch:"timestamp"`
	TraceID            string    `ch:"trace_id"`
	SpanID             string    `ch:"span_id"`
	Service            string    `ch:"service"`
	Environment        string    `ch:"environment"`
	Provider           string    `ch:"provider"`
	Model              string    `ch:"model"`
	Operation          string    `ch:"operation"`
	Name               string    `ch:"name"`
	PromptName         string    `ch:"prompt_name"`
	PromptVersion      string    `ch:"prompt_version"`
	AgentName          string    `ch:"agent_name"`
	ToolName           string    `ch:"tool_name"`
	DataSource         string    `ch:"data_source"`
	DurationMs         float64   `ch:"duration_ms"`
	TotalTokens        uint64    `ch:"total_tokens"`
	EstimatedTotalCost float64   `ch:"estimated_total_cost"`
	Status             string    `ch:"status"`
	ErrorType          string    `ch:"error_type"`
}

type agentRunRow struct {
	Timestamp time.Time `ch:"timestamp"`
	AgentName string    `ch:"agent_name"`
	Operation string    `ch:"operation"`
	Runs      uint64    `ch:"runs"`
	Errors    uint64    `ch:"errors"`
}

type toolCallRow struct {
	Timestamp time.Time `ch:"timestamp"`
	ToolName  string    `ch:"tool_name"`
	ToolType  string    `ch:"tool_type"`
	Calls     uint64    `ch:"calls"`
}

type toolErrorRow struct {
	Timestamp time.Time `ch:"timestamp"`
	ToolName  string    `ch:"tool_name"`
	ErrorType string    `ch:"error_type"`
	Errors    uint64    `ch:"errors"`
}

type toolLatencyRow struct {
	Timestamp time.Time `ch:"timestamp"`
	ToolName  string    `ch:"tool_name"`
	ToolType  string    `ch:"tool_type"`
	AvgMs     float64   `ch:"avg_ms"`
	P50Ms     float64   `ch:"p50_ms"`
	P95Ms     float64   `ch:"p95_ms"`
	P99Ms     float64   `ch:"p99_ms"`
}

type retrievalRateRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	DataSource string    `ch:"data_source"`
	Provider   string    `ch:"provider"`
	Requests   uint64    `ch:"requests"`
	Rate       float64   `ch:"rate"`
}

type retrievalLatencyRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	DataSource string    `ch:"data_source"`
	Provider   string    `ch:"provider"`
	AvgMs      float64   `ch:"avg_ms"`
	P50Ms      float64   `ch:"p50_ms"`
	P95Ms      float64   `ch:"p95_ms"`
	P99Ms      float64   `ch:"p99_ms"`
}

type retrievalErrorRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	DataSource string    `ch:"data_source"`
	Provider   string    `ch:"provider"`
	Requests   uint64    `ch:"requests"`
	Errors     uint64    `ch:"errors"`
	ErrorRate  float64   `ch:"error_rate"`
}

type facetRow struct {
	Dim   string `ch:"dim"`
	Value string `ch:"value"`
	Count uint64 `ch:"count"`
}
