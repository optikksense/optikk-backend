package aiobservability

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetLLMRequestRateByModel(ctx context.Context, q QueryOptions) ([]ModelRatePoint, error) {
	rows, err := s.repo.QueryLLMRequestRateByModel(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]ModelRatePoint, len(rows))
	for i, r := range rows {
		out[i] = ModelRatePoint{
			Timestamp: formatTime(r.Timestamp),
			Provider:  r.Provider,
			Model:     r.Model,
			Operation: r.Operation,
			Requests:  r.Requests,
			Rate:      clean(r.Rate),
		}
	}
	return out, nil
}

func (s *Service) GetLLMLatencyByModel(ctx context.Context, q QueryOptions) ([]ModelLatencyPoint, error) {
	rows, err := s.repo.QueryLLMLatencyByModel(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]ModelLatencyPoint, len(rows))
	for i, r := range rows {
		out[i] = ModelLatencyPoint{
			Timestamp: formatTime(r.Timestamp),
			Provider:  r.Provider,
			Model:     r.Model,
			Operation: r.Operation,
			AvgMs:     clean(r.AvgMs),
			P50Ms:     clean(r.P50Ms),
			P95Ms:     clean(r.P95Ms),
			P99Ms:     clean(r.P99Ms),
		}
	}
	return out, nil
}

func (s *Service) GetLLMErrorRateByModel(ctx context.Context, q QueryOptions) ([]ModelErrorRatePoint, error) {
	rows, err := s.repo.QueryLLMErrorRateByModel(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]ModelErrorRatePoint, len(rows))
	for i, r := range rows {
		out[i] = ModelErrorRatePoint{
			Timestamp: formatTime(r.Timestamp),
			Provider:  r.Provider,
			Model:     r.Model,
			Operation: r.Operation,
			Requests:  r.Requests,
			Errors:    r.Errors,
			ErrorRate: clean(r.ErrorRate),
		}
	}
	return out, nil
}

func (s *Service) GetLLMTokenUsageByModel(ctx context.Context, q QueryOptions) ([]TokenUsagePoint, error) {
	rows, err := s.repo.QueryLLMTokenUsageByModel(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	return tokenUsagePoints(rows, true), nil
}

func (s *Service) GetLLMTokenUsageByProvider(ctx context.Context, q QueryOptions) ([]TokenUsagePoint, error) {
	rows, err := s.repo.QueryLLMTokenUsageByProvider(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	return tokenUsagePoints(rows, false), nil
}

func (s *Service) GetLLMCostByModel(ctx context.Context, q QueryOptions) ([]CostPoint, error) {
	rows, err := s.repo.QueryLLMCostByModel(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	return costPoints(rows, true), nil
}

func (s *Service) GetLLMCostByProvider(ctx context.Context, q QueryOptions) ([]CostPoint, error) {
	rows, err := s.repo.QueryLLMCostByProvider(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	return costPoints(rows, false), nil
}

func (s *Service) GetTopExpensiveCalls(ctx context.Context, q QueryOptions) ([]CallRow, error) {
	rows, err := s.repo.QueryTopExpensiveCalls(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	return callRows(rows), nil
}

func (s *Service) GetTopSlowCalls(ctx context.Context, q QueryOptions) ([]CallRow, error) {
	rows, err := s.repo.QueryTopSlowCalls(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	return callRows(rows), nil
}

func (s *Service) GetPromptUsageByPrompt(ctx context.Context, q QueryOptions) ([]PromptUsagePoint, error) {
	rows, err := s.repo.QueryPromptUsageByPrompt(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]PromptUsagePoint, len(rows))
	for i, r := range rows {
		out[i] = PromptUsagePoint{Timestamp: formatTime(r.Timestamp), PromptName: r.PromptName, Calls: r.Calls}
	}
	return out, nil
}

func (s *Service) GetPromptUsageByVersion(ctx context.Context, q QueryOptions) ([]PromptUsagePoint, error) {
	rows, err := s.repo.QueryPromptUsageByVersion(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]PromptUsagePoint, len(rows))
	for i, r := range rows {
		out[i] = PromptUsagePoint{
			Timestamp:     formatTime(r.Timestamp),
			PromptName:    r.PromptName,
			PromptVersion: r.PromptVersion,
			Calls:         r.Calls,
		}
	}
	return out, nil
}

func (s *Service) GetPromptLatencyByVersion(ctx context.Context, q QueryOptions) ([]PromptLatencyPoint, error) {
	rows, err := s.repo.QueryPromptLatencyByVersion(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]PromptLatencyPoint, len(rows))
	for i, r := range rows {
		out[i] = PromptLatencyPoint{
			Timestamp:     formatTime(r.Timestamp),
			PromptName:    r.PromptName,
			PromptVersion: r.PromptVersion,
			AvgMs:         clean(r.AvgMs),
			P50Ms:         clean(r.P50Ms),
			P95Ms:         clean(r.P95Ms),
			P99Ms:         clean(r.P99Ms),
		}
	}
	return out, nil
}

func (s *Service) GetPromptTokenUsageByVersion(ctx context.Context, q QueryOptions) ([]PromptTokenUsagePoint, error) {
	rows, err := s.repo.QueryPromptTokenUsageByVersion(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]PromptTokenUsagePoint, len(rows))
	for i, r := range rows {
		out[i] = PromptTokenUsagePoint{
			Timestamp:     formatTime(r.Timestamp),
			PromptName:    r.PromptName,
			PromptVersion: r.PromptVersion,
			InputTokens:   r.InputTokens,
			OutputTokens:  r.OutputTokens,
			TotalTokens:   r.TotalTokens,
		}
	}
	return out, nil
}

func (s *Service) GetPromptCostByVersion(ctx context.Context, q QueryOptions) ([]PromptCostPoint, error) {
	rows, err := s.repo.QueryPromptCostByVersion(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]PromptCostPoint, len(rows))
	for i, r := range rows {
		out[i] = PromptCostPoint{
			Timestamp:           formatTime(r.Timestamp),
			PromptName:          r.PromptName,
			PromptVersion:       r.PromptVersion,
			EstimatedInputCost:  clean(r.EstimatedInputCost),
			EstimatedOutputCost: clean(r.EstimatedOutputCost),
			EstimatedTotalCost:  clean(r.EstimatedTotalCost),
		}
	}
	return out, nil
}

func (s *Service) GetPromptTraces(ctx context.Context, q QueryOptions) ([]TraceRow, error) {
	rows, err := s.repo.QueryPromptTraces(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	return traceRows(rows), nil
}

func (s *Service) GetAgentRunsByAgent(ctx context.Context, q QueryOptions) ([]AgentRunPoint, error) {
	rows, err := s.repo.QueryAgentRunsByAgent(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]AgentRunPoint, len(rows))
	for i, r := range rows {
		out[i] = AgentRunPoint{Timestamp: formatTime(r.Timestamp), AgentName: r.AgentName, Operation: r.Operation, Runs: r.Runs, Errors: r.Errors}
	}
	return out, nil
}

func (s *Service) GetToolCallsByTool(ctx context.Context, q QueryOptions) ([]ToolCallPoint, error) {
	rows, err := s.repo.QueryToolCallsByTool(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]ToolCallPoint, len(rows))
	for i, r := range rows {
		out[i] = ToolCallPoint{Timestamp: formatTime(r.Timestamp), ToolName: r.ToolName, ToolType: r.ToolType, Calls: r.Calls}
	}
	return out, nil
}

func (s *Service) GetToolErrorsByTool(ctx context.Context, q QueryOptions) ([]ToolErrorPoint, error) {
	rows, err := s.repo.QueryToolErrorsByTool(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]ToolErrorPoint, len(rows))
	for i, r := range rows {
		out[i] = ToolErrorPoint{Timestamp: formatTime(r.Timestamp), ToolName: r.ToolName, ErrorType: r.ErrorType, Errors: r.Errors}
	}
	return out, nil
}

func (s *Service) GetToolLatencyByTool(ctx context.Context, q QueryOptions) ([]ToolLatencyPoint, error) {
	rows, err := s.repo.QueryToolLatencyByTool(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]ToolLatencyPoint, len(rows))
	for i, r := range rows {
		out[i] = ToolLatencyPoint{
			Timestamp: formatTime(r.Timestamp),
			ToolName:  r.ToolName,
			ToolType:  r.ToolType,
			AvgMs:     clean(r.AvgMs),
			P50Ms:     clean(r.P50Ms),
			P95Ms:     clean(r.P95Ms),
			P99Ms:     clean(r.P99Ms),
		}
	}
	return out, nil
}

func (s *Service) GetRetrievalRequestRateByStore(ctx context.Context, q QueryOptions) ([]RetrievalRatePoint, error) {
	rows, err := s.repo.QueryRetrievalRequestRateByStore(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]RetrievalRatePoint, len(rows))
	for i, r := range rows {
		out[i] = RetrievalRatePoint{
			Timestamp:  formatTime(r.Timestamp),
			DataSource: r.DataSource,
			Provider:   r.Provider,
			Requests:   r.Requests,
			Rate:       clean(r.Rate),
		}
	}
	return out, nil
}

func (s *Service) GetRetrievalLatencyByStore(ctx context.Context, q QueryOptions) ([]RetrievalLatencyPoint, error) {
	rows, err := s.repo.QueryRetrievalLatencyByStore(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]RetrievalLatencyPoint, len(rows))
	for i, r := range rows {
		out[i] = RetrievalLatencyPoint{
			Timestamp:  formatTime(r.Timestamp),
			DataSource: r.DataSource,
			Provider:   r.Provider,
			AvgMs:      clean(r.AvgMs),
			P50Ms:      clean(r.P50Ms),
			P95Ms:      clean(r.P95Ms),
			P99Ms:      clean(r.P99Ms),
		}
	}
	return out, nil
}

func (s *Service) GetRetrievalErrorsByStore(ctx context.Context, q QueryOptions) ([]RetrievalErrorPoint, error) {
	rows, err := s.repo.QueryRetrievalErrorsByStore(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	out := make([]RetrievalErrorPoint, len(rows))
	for i, r := range rows {
		out[i] = RetrievalErrorPoint{
			Timestamp:  formatTime(r.Timestamp),
			DataSource: r.DataSource,
			Provider:   r.Provider,
			Requests:   r.Requests,
			Errors:     r.Errors,
			ErrorRate:  clean(r.ErrorRate),
		}
	}
	return out, nil
}

func (s *Service) QueryTraces(ctx context.Context, q QueryOptions) ([]TraceRow, error) {
	rows, err := s.repo.QueryTraces(ctx, clampRange(q))
	if err != nil {
		return nil, err
	}
	return traceRows(rows), nil
}

func (s *Service) GetFacets(ctx context.Context, q QueryOptions) (Facets, error) {
	rows, err := s.repo.QueryFacets(ctx, clampRange(q))
	if err != nil {
		return Facets{}, err
	}
	var out Facets
	for _, r := range rows {
		b := FacetBucket{Value: r.Value, Count: r.Count}
		switch r.Dim {
		case "provider":
			out.Providers = append(out.Providers, b)
		case "model":
			out.Models = append(out.Models, b)
		case "operation":
			out.Operations = append(out.Operations, b)
		case "service":
			out.Services = append(out.Services, b)
		case "environment":
			out.Environments = append(out.Environments, b)
		case "prompt_name":
			out.PromptNames = append(out.PromptNames, b)
		case "agent_name":
			out.AgentNames = append(out.AgentNames, b)
		case "tool_name":
			out.ToolNames = append(out.ToolNames, b)
		case "data_source":
			out.DataSources = append(out.DataSources, b)
		}
	}
	return out, nil
}

func tokenUsagePoints(rows []tokenUsageRow, includeModel bool) []TokenUsagePoint {
	out := make([]TokenUsagePoint, len(rows))
	for i, r := range rows {
		p := TokenUsagePoint{
			Timestamp:    formatTime(r.Timestamp),
			Provider:     r.Provider,
			InputTokens:  uintFromFloat(r.InputTokens),
			OutputTokens: uintFromFloat(r.OutputTokens),
			TotalTokens:  uintFromFloat(r.TotalTokens),
		}
		if includeModel {
			p.Model = r.Model
			p.Operation = r.Operation
		}
		out[i] = p
	}
	return out
}

func costPoints(rows []costRow, includeModel bool) []CostPoint {
	out := make([]CostPoint, len(rows))
	for i, r := range rows {
		p := CostPoint{
			Timestamp:           formatTime(r.Timestamp),
			Provider:            r.Provider,
			EstimatedInputCost:  clean(r.EstimatedInputCost),
			EstimatedOutputCost: clean(r.EstimatedOutputCost),
			EstimatedTotalCost:  clean(r.EstimatedTotalCost),
		}
		if includeModel {
			p.Model = r.Model
			p.Operation = r.Operation
		}
		out[i] = p
	}
	return out
}

func callRows(rows []callScanRow) []CallRow {
	out := make([]CallRow, len(rows))
	for i, r := range rows {
		out[i] = CallRow{
			Timestamp:          formatTime(r.Timestamp),
			TraceID:            r.TraceID,
			SpanID:             r.SpanID,
			Service:            r.Service,
			Environment:        r.Environment,
			Provider:           r.Provider,
			Model:              r.Model,
			Operation:          r.Operation,
			Name:               r.Name,
			DurationMs:         clean(r.DurationMs),
			InputTokens:        r.InputTokens,
			OutputTokens:       r.OutputTokens,
			TotalTokens:        r.TotalTokens,
			EstimatedTotalCost: clean(r.EstimatedTotalCost),
			Status:             r.Status,
			ErrorType:          r.ErrorType,
		}
	}
	return out
}

func traceRows(rows []traceScanRow) []TraceRow {
	out := make([]TraceRow, len(rows))
	for i, r := range rows {
		out[i] = TraceRow{
			Timestamp:          formatTime(r.Timestamp),
			TraceID:            r.TraceID,
			SpanID:             r.SpanID,
			Service:            r.Service,
			Environment:        r.Environment,
			Provider:           r.Provider,
			Model:              r.Model,
			Operation:          r.Operation,
			Name:               r.Name,
			PromptName:         r.PromptName,
			PromptVersion:      r.PromptVersion,
			AgentName:          r.AgentName,
			ToolName:           r.ToolName,
			DataSource:         r.DataSource,
			DurationMs:         clean(r.DurationMs),
			TotalTokens:        r.TotalTokens,
			EstimatedTotalCost: clean(r.EstimatedTotalCost),
			Status:             r.Status,
			ErrorType:          r.ErrorType,
		}
	}
	return out
}

func clampRange(q QueryOptions) QueryOptions {
	if q.EndMs-q.StartMs > maxTimeRangeMs {
		q.StartMs = q.EndMs - maxTimeRangeMs
	}
	if q.Limit <= 0 {
		q.Limit = 100
	}
	if q.Limit > 200 {
		q.Limit = 200
	}
	return q
}

func clean(v float64) float64 {
	return utils.SanitizeFloat(v)
}

func uintFromFloat(v float64) uint64 {
	v = clean(v)
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

func formatTime(t time.Time) string {
	return timebucket.FormatDisplayBucket(t)
}
