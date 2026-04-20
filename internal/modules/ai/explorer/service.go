package explorer

import (
	"context"
	"fmt"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
)

// Service orchestrates the AI explorer query: search, facets, trend, and summary.
type Service struct {
	repo Repository
}

// NewService creates a new AI explorer service.
func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// Query executes the full AI explorer query and assembles the response.
func (s *Service) Query(ctx context.Context, req QueryRequest, teamID int64) (Response, error) {
	filters, err := parseQueryToFilters(req.Query)
	if err != nil {
		return Response{}, fmt.Errorf("ai.Query.parseQuery: %w", err)
	}

	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	rows, total, err := s.repo.GetAICalls(ctx, teamID, req.StartTime, req.EndTime, filters, limit, req.Offset)
	if err != nil {
		return Response{}, fmt.Errorf("ai.Query.GetAICalls: %w", err)
	}

	summary, err := s.repo.GetAISummary(ctx, teamID, req.StartTime, req.EndTime, filters)
	if err != nil {
		return Response{}, fmt.Errorf("ai.Query.GetAISummary: %w", err)
	}

	facetRows, err := s.repo.GetAIFacets(ctx, teamID, req.StartTime, req.EndTime, filters)
	if err != nil {
		return Response{}, fmt.Errorf("ai.Query.GetAIFacets: %w", err)
	}

	trend, err := s.repo.GetAITrend(ctx, teamID, req.StartTime, req.EndTime, filters, req.Step)
	if err != nil {
		return Response{}, fmt.Errorf("ai.Query.GetAITrend: %w", err)
	}

	return Response{
		Results:  toAICalls(rows),
		Summary:  toAISummary(summary),
		Facets:   groupFacets(facetRows),
		Trend:    toAITrend(trend),
		PageInfo: PageInfo{Total: total, Offset: req.Offset, Limit: limit},
	}, nil
}

// QuerySessions returns paginated session aggregates for the LLM hub.
func (s *Service) QuerySessions(ctx context.Context, req SessionsQueryRequest, teamID int64) (SessionsResponse, error) {
	filters, err := parseQueryToFilters(req.Query)
	if err != nil {
		return SessionsResponse{}, fmt.Errorf("ai.QuerySessions.parseQuery: %w", err)
	}

	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	rows, total, err := s.repo.GetAISessions(ctx, teamID, req.StartTime, req.EndTime, filters, limit, req.Offset)
	if err != nil {
		return SessionsResponse{}, fmt.Errorf("ai.QuerySessions.GetAISessions: %w", err)
	}

	return SessionsResponse{
		Results:  toSessionRows(rows),
		PageInfo: PageInfo{Total: total, Offset: req.Offset, Limit: limit},
	}, nil
}

// parseQueryToFilters uses the shared queryparser to extract attribute filters
// from the user's query string. Known field names (service, status, model, provider,
// operation) are mapped to their gen_ai attribute keys.
func parseQueryToFilters(query string) ([]attrFilter, error) {
	if query == "" {
		return nil, nil
	}

	node, err := queryparser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}
	if node == nil {
		return nil, nil
	}

	var filters []attrFilter
	extractFilters(node, &filters)
	return filters, nil
}

func extractFilters(node queryparser.Node, filters *[]attrFilter) {
	switch n := node.(type) {
	case *queryparser.AndNode:
		for _, child := range n.Children {
			extractFilters(child, filters)
		}
	case *queryparser.FieldMatch:
		af := mapAIField(n.Field, n.Value)
		if af.Key != "" {
			*filters = append(*filters, af)
		}
	case *queryparser.ExistsMatch:
		af := mapAIFieldExists(n.Field)
		if af.Key != "" {
			*filters = append(*filters, af)
		}
	}
}

// mapAIField converts a user-facing field name to an attribute filter.
func mapAIField(field, value string) attrFilter {
	lower := strings.ToLower(field)
	switch lower {
	case "service", "service_name":
		// Service uses a materialized column, but for the AI explorer we still
		// filter via the WHERE clause using the standard column. We represent
		// this as a special-case that the repository handles. For simplicity,
		// we use the same attrFilter mechanism but with a "service_name" key
		// that the WHERE builder recognises.
		return attrFilter{Key: "__service_name", Value: value, Op: "eq"}
	case "status":
		return attrFilter{Key: "__status", Value: value, Op: "eq"}
	case "model", "ai_model", "gen_ai.request.model":
		return attrFilter{Key: "gen_ai.request.model", Value: value, Op: "eq"}
	case "provider", "ai_system", "gen_ai.system":
		return attrFilter{Key: "gen_ai.system", Value: value, Op: "eq"}
	case "operation", "ai_operation", "gen_ai.operation.name":
		return attrFilter{Key: "gen_ai.operation.name", Value: value, Op: "eq"}
	case "finish_reason":
		return attrFilter{Key: "gen_ai.response.finish_reasons", Value: value, Op: "eq"}
	case "session", "session_id", "conversation":
		return attrFilter{Key: "__session_id", Value: value, Op: "eq"}
	case "prompt", "prompt_template", "gen_ai.prompt.template.name":
		return attrFilter{Key: "gen_ai.prompt.template.name", Value: value, Op: "eq"}
	default:
		if strings.HasPrefix(field, "@") {
			return attrFilter{Key: field[1:], Value: value, Op: "eq"}
		}
		return attrFilter{}
	}
}

func mapAIFieldExists(field string) attrFilter {
	lower := strings.ToLower(field)
	switch lower {
	case "model", "ai_model", "gen_ai.request.model":
		return attrFilter{Key: "gen_ai.request.model", Op: "exists"}
	case "provider", "ai_system", "gen_ai.system":
		return attrFilter{Key: "gen_ai.system", Op: "exists"}
	default:
		if strings.HasPrefix(field, "@") {
			return attrFilter{Key: field[1:], Op: "exists"}
		}
		return attrFilter{}
	}
}

// --- Converters from internal rows to DTO types ---

func toAICalls(rows []aiCallRow) []AICall {
	calls := make([]AICall, 0, len(rows))
	for _, r := range rows {
		totalTokens := r.TotalTokens
		if totalTokens == 0 {
			totalTokens = r.InputTokens + r.OutputTokens
		}
		calls = append(calls, AICall{
			SpanID:          r.SpanID,
			TraceID:         r.TraceID,
			ServiceName:     r.ServiceName,
			OperationName:   r.OperationName,
			StartTime:       r.StartTime,
			DurationMs:      r.DurationMs,
			Status:          r.Status,
			StatusMessage:   r.StatusMessage,
			AISystem:        r.AISystem,
			AIRequestModel:  r.AIRequestModel,
			AIResponseModel: r.AIResponseModel,
			AIOperation:     r.AIOperation,
			InputTokens:     r.InputTokens,
			OutputTokens:    r.OutputTokens,
			TotalTokens:     totalTokens,
			Temperature:     r.Temperature,
			MaxTokens:       r.MaxTokens,
			FinishReason:    r.FinishReason,
			ErrorType:       r.ErrorType,
		})
	}
	return calls
}

func toAISummary(row aiSummaryRow) AISummary {
	return AISummary{
		TotalCalls:        row.TotalCalls,
		ErrorCalls:        row.ErrorCalls,
		AvgLatencyMs:      row.AvgLatencyMs,
		P50LatencyMs:      row.P50LatencyMs,
		P95LatencyMs:      row.P95LatencyMs,
		P99LatencyMs:      row.P99LatencyMs,
		TotalInputTokens:  row.TotalInputTokens,
		TotalOutputTokens: row.TotalOutputTokens,
	}
}

func groupFacets(rows []aiFacetRow) AIExplorerFacets {
	facets := AIExplorerFacets{
		AISystem:       []FacetBucket{},
		AIModel:        []FacetBucket{},
		AIOperation:    []FacetBucket{},
		ServiceName:    []FacetBucket{},
		Status:         []FacetBucket{},
		FinishReason:   []FacetBucket{},
		PromptTemplate: []FacetBucket{},
	}
	for _, r := range rows {
		bucket := FacetBucket{Value: r.FacetValue, Count: r.Count}
		switch r.FacetKey {
		case "ai_system":
			facets.AISystem = append(facets.AISystem, bucket)
		case "ai_model":
			facets.AIModel = append(facets.AIModel, bucket)
		case "ai_operation":
			facets.AIOperation = append(facets.AIOperation, bucket)
		case "service_name":
			facets.ServiceName = append(facets.ServiceName, bucket)
		case "status":
			facets.Status = append(facets.Status, bucket)
		case "finish_reason":
			facets.FinishReason = append(facets.FinishReason, bucket)
		case "prompt_template":
			facets.PromptTemplate = append(facets.PromptTemplate, bucket)
		}
	}
	return facets
}

func toSessionRows(rows []aiSessionRow) []SessionRow {
	out := make([]SessionRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, SessionRow{
			SessionID:         r.SessionID,
			GenerationCount:   r.GenerationCount,
			TraceCount:        r.TraceCount,
			FirstStart:        r.FirstStart,
			LastStart:         r.LastStart,
			TotalInputTokens:  r.TotalInputTokens,
			TotalOutputTokens: r.TotalOutputTokens,
			ErrorCount:        r.ErrorCount,
			DominantModel:     r.DominantModel,
			DominantService:   r.DominantService,
		})
	}
	return out
}

func toAITrend(rows []aiTrendRow) []AITrendBucket {
	trend := make([]AITrendBucket, 0, len(rows))
	for _, r := range rows {
		trend = append(trend, AITrendBucket{
			TimeBucket:   r.TimeBucket,
			TotalCalls:   r.TotalCalls,
			ErrorCalls:   r.ErrorCalls,
			AvgLatencyMs: r.AvgLatencyMs,
			TotalTokens:  r.TotalTokens,
		})
	}
	return trend
}
