package explorer

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
)

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

// Service orchestrates the AI explorer query: search, facets, trend, and summary.
type Service struct {
	repo    Repository
	sketchQ *sketch.Querier
}

// NewService creates a new AI explorer service.
func NewService(repo Repository, sketchQ *sketch.Querier) *Service {
	return &Service{repo: repo, sketchQ: sketchQ}
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
	s.attachSummaryPercentiles(ctx, teamID, req.StartTime, req.EndTime, &summary)

	facetRows, err := s.repo.GetAIFacets(ctx, teamID, req.StartTime, req.EndTime, filters)
	if err != nil {
		return Response{}, fmt.Errorf("ai.Query.GetAIFacets: %w", err)
	}

	trend, err := s.repo.GetAITrend(ctx, teamID, req.StartTime, req.EndTime, filters, req.Step)
	if err != nil {
		return Response{}, fmt.Errorf("ai.Query.GetAITrend: %w", err)
	}
	for i := range trend {
		if trend[i].LatencyMsCount > 0 {
			trend[i].AvgLatencyMs = trend[i].LatencyMsSum / float64(trend[i].LatencyMsCount)
		}
	}

	return Response{
		Results:  toAICalls(rows),
		Summary:  toAISummary(summary),
		Facets:   groupFacets(facetRows),
		Trend:    toAITrend(trend),
		PageInfo: PageInfo{Total: total, Offset: req.Offset, Limit: limit},
	}, nil
}

// QuerySessions returns paginated session aggregates for the LLM hub. The
// repository returns raw per-span rows; grouping + sort + paging happen here
// so the SQL can stay free of branching/conditional-aggregator combinators.
func (s *Service) QuerySessions(ctx context.Context, req SessionsQueryRequest, teamID int64) (SessionsResponse, error) {
	filters, err := parseQueryToFilters(req.Query)
	if err != nil {
		return SessionsResponse{}, fmt.Errorf("ai.QuerySessions.parseQuery: %w", err)
	}

	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	rawRows, err := s.repo.GetAISessions(ctx, teamID, req.StartTime, req.EndTime, filters, limit, req.Offset)
	if err != nil {
		return SessionsResponse{}, fmt.Errorf("ai.QuerySessions.GetAISessions: %w", err)
	}

	sessions := aggregateSessions(rawRows)
	total := uint64(len(sessions))

	// Paginate the aggregated slice.
	start := req.Offset
	if start < 0 {
		start = 0
	}
	if start > len(sessions) {
		start = len(sessions)
	}
	end := start + limit
	if end > len(sessions) {
		end = len(sessions)
	}
	paged := sessions[start:end]

	return SessionsResponse{
		Results:  toSessionRows(paged),
		PageInfo: PageInfo{Total: total, Offset: req.Offset, Limit: limit},
	}, nil
}

// pickSessionID returns the first non-empty session id candidate, replacing
// the branching SQL expression that used to pick this in-database.
func pickSessionID(primary, secondary, tertiary string) string {
	if primary != "" {
		return primary
	}
	if secondary != "" {
		return secondary
	}
	return tertiary
}

// aggregateSessions groups raw GenAI span rows into session aggregates. The
// rows are expected to be ordered by timestamp DESC so the latest row's
// model/service wins as the "dominant" representative (argMax equivalent).
func aggregateSessions(rows []aiSessionRawRow) []aiSessionRow {
	type agg struct {
		sessionID         string
		genCount          uint64
		traceSet          map[string]struct{}
		firstStart        int64
		lastStart         int64
		totalIn           float64
		totalOut          float64
		errorCount        uint64
		dominantModel     string
		dominantService   string
		dominantTimestamp int64
	}

	groups := make(map[string]*agg)
	for _, r := range rows {
		sid := pickSessionID(r.SessionIDPrimary, r.SessionIDSecondary, r.SessionIDTertiary)
		if sid == "" {
			continue
		}
		g, ok := groups[sid]
		if !ok {
			g = &agg{sessionID: sid, traceSet: map[string]struct{}{}, firstStart: r.StartTime.UnixNano(), lastStart: r.StartTime.UnixNano()}
			groups[sid] = g
		}
		g.genCount++
		if r.TraceID != "" {
			g.traceSet[r.TraceID] = struct{}{}
		}
		ts := r.StartTime.UnixNano()
		if ts < g.firstStart {
			g.firstStart = ts
		}
		if ts > g.lastStart {
			g.lastStart = ts
		}
		g.totalIn += parseFloatOrZero(r.InputTokensRaw)
		g.totalOut += parseFloatOrZero(r.OutputTokensRaw)
		if r.Status == "ERROR" || r.HasError {
			g.errorCount++
		}
		if ts >= g.dominantTimestamp {
			g.dominantTimestamp = ts
			g.dominantModel = r.AIRequestModel
			g.dominantService = r.ServiceName
		}
	}

	out := make([]aiSessionRow, 0, len(groups))
	for _, g := range groups {
		out = append(out, aiSessionRow{
			SessionID:         g.sessionID,
			GenerationCount:   g.genCount,
			TraceCount:        uint64(len(g.traceSet)),
			FirstStart:        time.Unix(0, g.firstStart),
			LastStart:         time.Unix(0, g.lastStart),
			TotalInputTokens:  g.totalIn,
			TotalOutputTokens: g.totalOut,
			ErrorCount:        g.errorCount,
			DominantModel:     g.dominantModel,
			DominantService:   g.dominantService,
		})
	}
	// Match the old ORDER BY last_start DESC.
	sort.Slice(out, func(i, j int) bool { return out[i].LastStart.After(out[j].LastStart) })
	return out
}

// parseFloatOrZero parses a raw JSON attribute string into a float64. Empty
// or malformed values yield 0, matching the old or-zero cast behaviour.
func parseFloatOrZero(s string) float64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
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
		input := parseFloatOrZero(r.InputTokensRaw)
		output := parseFloatOrZero(r.OutputTokensRaw)
		total := parseFloatOrZero(r.TotalTokensRaw)
		if total == 0 {
			total = input + output
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
			InputTokens:     input,
			OutputTokens:    output,
			TotalTokens:     total,
			Temperature:     r.Temperature,
			MaxTokens:       r.MaxTokens,
			FinishReason:    r.FinishReason,
			ErrorType:       r.ErrorType,
		})
	}
	return calls
}

func toAISummary(row aiSummaryRow) AISummary {
	avg := row.AvgLatencyMs
	if avg == 0 && row.LatencyMsCount > 0 {
		avg = row.LatencyMsSum / float64(row.LatencyMsCount)
	}
	return AISummary{
		TotalCalls:        row.TotalCalls,
		ErrorCalls:        row.ErrorCalls,
		AvgLatencyMs:      avg,
		P50LatencyMs:      row.P50LatencyMs,
		P95LatencyMs:      row.P95LatencyMs,
		P99LatencyMs:      row.P99LatencyMs,
		TotalInputTokens:  row.TotalInputTokens,
		TotalOutputTokens: row.TotalOutputTokens,
	}
}

// attachSummaryPercentiles fills p50/p95/p99 from SpanLatencyService sketches
// for the service names present in the window. Caveat: SpanLatencyService dim
// is not GenAI-filtered, so a service that mixes AI and non-AI traffic will
// contribute all its latencies here. Documented trade-off.
func (s *Service) attachSummaryPercentiles(ctx context.Context, teamID, startMs, endMs int64, row *aiSummaryRow) {
	if s.sketchQ == nil || row == nil {
		return
	}
	prefixes := []string{""}
	if len(row.Services) > 0 {
		prefixes = prefixes[:0]
		for _, svc := range row.Services {
			if svc != "" {
				prefixes = append(prefixes, sketch.DimSpanService(svc))
			}
		}
		if len(prefixes) == 0 {
			prefixes = []string{""}
		}
	}
	pcts, err := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, prefixes, 0.5, 0.95, 0.99)
	if err != nil {
		return
	}
	merged := make([]float64, 3)
	for _, v := range pcts {
		if len(v) != 3 {
			continue
		}
		for i := range merged {
			if v[i] > merged[i] {
				merged[i] = v[i]
			}
		}
	}
	row.P50LatencyMs = merged[0]
	row.P95LatencyMs = merged[1]
	row.P99LatencyMs = merged[2]
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
		avg := r.AvgLatencyMs
		if avg == 0 && r.LatencyMsCount > 0 {
			avg = r.LatencyMsSum / float64(r.LatencyMsCount)
		}
		trend = append(trend, AITrendBucket{
			TimeBucket:   r.TimeBucket,
			TotalCalls:   r.TotalCalls,
			ErrorCalls:   r.ErrorCalls,
			AvgLatencyMs: avg,
			TotalTokens:  r.TotalTokens,
		})
	}
	return trend
}
