package analytics

import "context"

// modelPricing maps model name prefixes to per-million-token costs (input, output).
// These are static estimates — the canonical reference for cost estimation.
var modelPricing = map[string][2]float64{
	"gpt-4o":          {2.50, 10.00},
	"gpt-4o-mini":     {0.15, 0.60},
	"gpt-4-turbo":     {10.00, 30.00},
	"gpt-4":           {30.00, 60.00},
	"gpt-3.5-turbo":   {0.50, 1.50},
	"claude-3-opus":   {15.00, 75.00},
	"claude-3-sonnet": {3.00, 15.00},
	"claude-3-haiku":  {0.25, 1.25},
	"claude-3.5":      {3.00, 15.00},
	"gemini-1.5-pro":  {3.50, 10.50},
	"gemini-1.5-flash": {0.35, 1.05},
	"command-r-plus":  {3.00, 15.00},
	"command-r":       {0.50, 1.50},
	"llama-3":         {0.00, 0.00}, // Self-hosted
}

func estimateCost(model string, inputTokens, outputTokens int64) float64 {
	for prefix, pricing := range modelPricing {
		if len(model) >= len(prefix) && model[:len(prefix)] == prefix {
			return (float64(inputTokens) * pricing[0] / 1_000_000) +
				(float64(outputTokens) * pricing[1] / 1_000_000)
		}
	}
	// Default: approximate average pricing
	return (float64(inputTokens)*2.0 + float64(outputTokens)*8.0) / 1_000_000
}

// Service defines the business logic contract for the AI analytics module.
type Service interface {
	GetModelCatalog(f AnalyticsFilter) ([]ModelCatalogDTO, error)
	GetLatencyDistribution(f AnalyticsFilter) ([]LatencyBucketDTO, error)
	GetParameterImpact(f AnalyticsFilter) ([]ParamImpactDTO, error)
	GetModelTimeseries(f AnalyticsFilter, model string) ([]ModelTimeseriesDTO, error)
	GetCostSummary(f AnalyticsFilter) (CostSummaryDTO, error)
	GetCostTimeseries(f AnalyticsFilter) ([]CostTimeseriesPointDTO, error)
	GetTokenEconomics(f AnalyticsFilter) (TokenEconomicsDTO, error)
	GetErrorPatterns(f AnalyticsFilter, limit int) ([]ErrorPatternDTO, error)
	GetErrorTimeseries(f AnalyticsFilter) ([]ErrorTimeseriesPointDTO, error)
	GetFinishReasonTrends(f AnalyticsFilter) ([]FinishReasonTrendDTO, error)
	GetConversations(f AnalyticsFilter, limit, offset int) ([]ConversationDTO, error)
	GetConversationTurns(teamID int64, conversationID string) ([]ConversationTurnDTO, error)
	GetConversationSummary(teamID int64, conversationID string) (ConversationSummaryDTO, error)
}

type analyticsService struct {
	repo Repository
}

// NewService creates a new analytics Service.
func NewService(repo Repository) Service {
	return &analyticsService{repo: repo}
}

func (s *analyticsService) GetModelCatalog(f AnalyticsFilter) ([]ModelCatalogDTO, error) {
	rows, err := s.repo.GetModelCatalog(context.Background(), f)
	if err != nil {
		return nil, err
	}
	durationSec := float64(f.EndMs-f.StartMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}
	result := make([]ModelCatalogDTO, len(rows))
	for i, r := range rows {
		total := r.InputTokens + r.OutputTokens
		tps := 0.0
		if durationSec > 0 && total > 0 {
			tps = float64(total) / durationSec
		}
		health := "healthy"
		if r.ErrorRate > 10 || r.P95Ms > 5000 {
			health = "critical"
		} else if r.ErrorRate > 3 || r.P95Ms > 3000 {
			health = "degraded"
		}
		result[i] = ModelCatalogDTO{
			Model:        r.Model,
			Provider:     r.Provider,
			RequestCount: r.RequestCount,
			AvgLatencyMs: r.AvgLatencyMs,
			P50Ms:        r.P50Ms,
			P95Ms:        r.P95Ms,
			P99Ms:        r.P99Ms,
			ErrorRate:    r.ErrorRate,
			InputTokens:  r.InputTokens,
			OutputTokens: r.OutputTokens,
			TotalTokens:  total,
			TokensPerSec: tps,
			TopOps:       r.TopOps,
			TopSvcs:      r.TopSvcs,
			EstCost:      estimateCost(r.Model, r.InputTokens, r.OutputTokens),
			Health:       health,
		}
	}
	return result, nil
}

func (s *analyticsService) GetLatencyDistribution(f AnalyticsFilter) ([]LatencyBucketDTO, error) {
	rows, err := s.repo.GetLatencyDistribution(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]LatencyBucketDTO, len(rows))
	for i, r := range rows {
		result[i] = LatencyBucketDTO{Model: r.Model, BucketMs: r.BucketMs, Count: r.Count}
	}
	return result, nil
}

func (s *analyticsService) GetParameterImpact(f AnalyticsFilter) ([]ParamImpactDTO, error) {
	rows, err := s.repo.GetParameterImpact(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]ParamImpactDTO, len(rows))
	for i, r := range rows {
		result[i] = ParamImpactDTO{Temperature: r.Temperature, AvgLatency: r.AvgLatency, ErrorRate: r.ErrorRate, Count: r.Count}
	}
	return result, nil
}

func (s *analyticsService) GetModelTimeseries(f AnalyticsFilter, model string) ([]ModelTimeseriesDTO, error) {
	rows, err := s.repo.GetModelTimeseries(context.Background(), f, model)
	if err != nil {
		return nil, err
	}
	result := make([]ModelTimeseriesDTO, len(rows))
	for i, r := range rows {
		result[i] = ModelTimeseriesDTO{
			Timestamp:    r.Timestamp,
			RequestCount: r.RequestCount,
			AvgLatencyMs: r.AvgLatencyMs,
			P95Ms:        r.P95Ms,
			ErrorRate:    r.ErrorRate,
			InputTokens:  r.InputTokens,
			OutputTokens: r.OutputTokens,
		}
	}
	return result, nil
}

func (s *analyticsService) GetCostSummary(f AnalyticsFilter) (CostSummaryDTO, error) {
	rows, err := s.repo.GetCostByModel(context.Background(), f)
	if err != nil {
		return CostSummaryDTO{}, err
	}
	var totalCost float64
	var totalInput, totalOutput int64
	byModel := make([]ModelCostDTO, len(rows))
	for i, r := range rows {
		cost := estimateCost(r.Model, r.InputTokens, r.OutputTokens)
		totalCost += cost
		totalInput += r.InputTokens
		totalOutput += r.OutputTokens
		byModel[i] = ModelCostDTO{
			Model:        r.Model,
			InputTokens:  r.InputTokens,
			OutputTokens: r.OutputTokens,
			EstCost:      cost,
		}
	}
	requestCount := int64(0)
	for _, r := range rows {
		requestCount += r.InputTokens // Approximate — use token count as proxy
	}
	costPerReq := 0.0
	if len(rows) > 0 {
		costPerReq = totalCost / float64(len(rows))
	}
	return CostSummaryDTO{
		TotalEstCost: totalCost,
		CostPerReq:   costPerReq,
		TotalInput:   totalInput,
		TotalOutput:  totalOutput,
		ByModel:      byModel,
	}, nil
}

func (s *analyticsService) GetCostTimeseries(f AnalyticsFilter) ([]CostTimeseriesPointDTO, error) {
	rows, err := s.repo.GetCostTimeseries(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]CostTimeseriesPointDTO, len(rows))
	for i, r := range rows {
		result[i] = CostTimeseriesPointDTO{
			Timestamp: r.Timestamp,
			Model:     r.Model,
			EstCost:   estimateCost(r.Model, r.InputTokens, r.OutputTokens),
		}
	}
	return result, nil
}

func (s *analyticsService) GetTokenEconomics(f AnalyticsFilter) (TokenEconomicsDTO, error) {
	row, err := s.repo.GetTokenEconomics(context.Background(), f)
	if err != nil {
		return TokenEconomicsDTO{}, err
	}
	ratio := 0.0
	if row.TotalOutput > 0 {
		ratio = float64(row.TotalInput) / float64(row.TotalOutput)
	}
	return TokenEconomicsDTO{
		TotalInput:     row.TotalInput,
		TotalOutput:    row.TotalOutput,
		InputOutputRat: ratio,
		AvgPerReq:      row.AvgPerReq,
		RequestCount:   row.RequestCount,
	}, nil
}

func (s *analyticsService) GetErrorPatterns(f AnalyticsFilter, limit int) ([]ErrorPatternDTO, error) {
	rows, err := s.repo.GetErrorPatterns(context.Background(), f, limit)
	if err != nil {
		return nil, err
	}
	result := make([]ErrorPatternDTO, len(rows))
	for i, r := range rows {
		result[i] = ErrorPatternDTO{
			Model:         r.Model,
			Operation:     r.Operation,
			StatusMessage: r.StatusMessage,
			ErrorCount:    r.ErrorCount,
			FirstSeen:     r.FirstSeen,
			LastSeen:      r.LastSeen,
		}
	}
	return result, nil
}

func (s *analyticsService) GetErrorTimeseries(f AnalyticsFilter) ([]ErrorTimeseriesPointDTO, error) {
	rows, err := s.repo.GetErrorTimeseries(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]ErrorTimeseriesPointDTO, len(rows))
	for i, r := range rows {
		result[i] = ErrorTimeseriesPointDTO{
			Timestamp:  r.Timestamp,
			Model:      r.Model,
			ErrorCount: r.ErrorCount,
			ErrorRate:  r.ErrorRate,
		}
	}
	return result, nil
}

func (s *analyticsService) GetFinishReasonTrends(f AnalyticsFilter) ([]FinishReasonTrendDTO, error) {
	rows, err := s.repo.GetFinishReasonTrends(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]FinishReasonTrendDTO, len(rows))
	for i, r := range rows {
		result[i] = FinishReasonTrendDTO{Timestamp: r.Timestamp, FinishReason: r.FinishReason, Count: r.Count}
	}
	return result, nil
}

func (s *analyticsService) GetConversations(f AnalyticsFilter, limit, offset int) ([]ConversationDTO, error) {
	rows, err := s.repo.GetConversations(context.Background(), f, limit, offset)
	if err != nil {
		return nil, err
	}
	result := make([]ConversationDTO, len(rows))
	for i, r := range rows {
		result[i] = ConversationDTO{
			ConversationID: r.ConversationID,
			ServiceName:    r.ServiceName,
			Model:          r.Model,
			TurnCount:      r.TurnCount,
			TotalTokens:    r.TotalTokens,
			HasError:       r.HasError,
			FirstTurn:      r.FirstTurn,
			LastTurn:       r.LastTurn,
		}
	}
	return result, nil
}

func (s *analyticsService) GetConversationTurns(teamID int64, conversationID string) ([]ConversationTurnDTO, error) {
	rows, err := s.repo.GetConversationTurns(context.Background(), teamID, conversationID)
	if err != nil {
		return nil, err
	}
	result := make([]ConversationTurnDTO, len(rows))
	for i, r := range rows {
		result[i] = ConversationTurnDTO{
			SpanID:       r.SpanID,
			Timestamp:    r.Timestamp,
			Model:        r.Model,
			DurationMs:   r.DurationMs,
			InputTokens:  r.InputTokens,
			OutputTokens: r.OutputTokens,
			TotalTokens:  r.InputTokens + r.OutputTokens,
			HasError:     r.HasError,
			FinishReason: r.FinishReason,
		}
	}
	return result, nil
}

func (s *analyticsService) GetConversationSummary(teamID int64, conversationID string) (ConversationSummaryDTO, error) {
	row, err := s.repo.GetConversationSummary(context.Background(), teamID, conversationID)
	if err != nil {
		return ConversationSummaryDTO{}, err
	}
	return ConversationSummaryDTO{
		TurnCount:   row.TurnCount,
		TotalTokens: row.TotalTokens,
		TotalMs:     row.TotalMs,
		Models:      row.Models,
		ErrorTurns:  row.ErrorTurns,
		FirstTurn:   row.FirstTurn,
		LastTurn:    row.LastTurn,
	}, nil
}
