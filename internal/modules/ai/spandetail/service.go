package spandetail

import (
	"context"
	"encoding/json"
)

// Service defines the business logic contract for the AI span detail module.
type Service interface {
	GetSpanDetail(teamID int64, spanID string) (SpanDetailDTO, error)
	GetMessages(teamID int64, spanID string) ([]MessageDTO, error)
	GetTraceContext(teamID int64, traceID string) ([]TraceContextSpanDTO, error)
	GetRelatedSpans(teamID int64, model, operation, excludeSpanID string, startMs, endMs int64) ([]RelatedSpanDTO, error)
	GetTokenBreakdown(teamID int64, spanID, model string, startMs, endMs int64) (TokenBreakdownDTO, error)
}

type spanDetailService struct {
	repo Repository
}

// NewService creates a new span detail Service.
func NewService(repo Repository) Service {
	return &spanDetailService{repo: repo}
}

func (s *spanDetailService) GetSpanDetail(teamID int64, spanID string) (SpanDetailDTO, error) {
	row, err := s.repo.GetSpanDetail(context.Background(), teamID, spanID)
	if err != nil {
		return SpanDetailDTO{}, err
	}
	total := row.InputTokens + row.OutputTokens
	tps := 0.0
	if row.DurationMs > 0 {
		tps = float64(row.OutputTokens) / (row.DurationMs / 1000.0)
	}
	return SpanDetailDTO{
		SpanID:           row.SpanID,
		TraceID:          row.TraceID,
		ParentSpanID:     row.ParentSpanID,
		ServiceName:      row.ServiceName,
		OperationName:    row.OperationName,
		KindString:       row.KindString,
		Timestamp:        row.Timestamp,
		DurationMs:       row.DurationMs,
		HasError:         row.HasError,
		StatusMessage:    row.StatusMessage,
		Model:            row.Model,
		ResponseModel:    row.ResponseModel,
		Provider:         row.Provider,
		OperationType:    row.OperationType,
		Temperature:      row.Temperature,
		TopP:             row.TopP,
		MaxTokens:        row.MaxTokens,
		FrequencyPenalty: row.FrequencyPenalty,
		PresencePenalty:  row.PresencePenalty,
		Seed:             row.Seed,
		InputTokens:      row.InputTokens,
		OutputTokens:     row.OutputTokens,
		TotalTokens:      total,
		TokensPerSec:     tps,
		FinishReason:     row.FinishReason,
		ResponseID:       row.ResponseID,
		ServerAddress:    row.ServerAddress,
		ConversationID:   row.ConversationID,
	}, nil
}

func (s *spanDetailService) GetMessages(teamID int64, spanID string) ([]MessageDTO, error) {
	rows, err := s.repo.GetMessages(context.Background(), teamID, spanID)
	if err != nil {
		return nil, err
	}
	var result []MessageDTO
	for _, r := range rows {
		// Try to parse JSON body for role/content
		var parsed map[string]string
		if err := json.Unmarshal([]byte(r.Body), &parsed); err == nil {
			role := parsed["role"]
			if role == "" {
				if r.EventName == "gen_ai.content.prompt" {
					role = "user"
				} else {
					role = "assistant"
				}
			}
			result = append(result, MessageDTO{
				Role:    role,
				Content: parsed["content"],
			})
		} else {
			// Fallback: treat entire body as content
			role := "user"
			if r.EventName == "gen_ai.content.completion" {
				role = "assistant"
			}
			result = append(result, MessageDTO{
				Role:    role,
				Content: r.Body,
			})
		}
	}
	return result, nil
}

func (s *spanDetailService) GetTraceContext(teamID int64, traceID string) ([]TraceContextSpanDTO, error) {
	rows, err := s.repo.GetTraceContext(context.Background(), teamID, traceID)
	if err != nil {
		return nil, err
	}
	result := make([]TraceContextSpanDTO, len(rows))
	for i, r := range rows {
		result[i] = TraceContextSpanDTO{
			SpanID:        r.SpanID,
			ParentSpanID:  r.ParentSpanID,
			ServiceName:   r.ServiceName,
			OperationName: r.OperationName,
			KindString:    r.KindString,
			Timestamp:     r.Timestamp,
			DurationMs:    r.DurationMs,
			HasError:      r.HasError,
			IsAI:          r.IsAI,
		}
	}
	return result, nil
}

func (s *spanDetailService) GetRelatedSpans(teamID int64, model, operation, excludeSpanID string, startMs, endMs int64) ([]RelatedSpanDTO, error) {
	rows, err := s.repo.GetRelatedSpans(context.Background(), teamID, model, operation, excludeSpanID, startMs, endMs, 10)
	if err != nil {
		return nil, err
	}
	result := make([]RelatedSpanDTO, len(rows))
	for i, r := range rows {
		result[i] = RelatedSpanDTO{
			SpanID:       r.SpanID,
			Timestamp:    r.Timestamp,
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

func (s *spanDetailService) GetTokenBreakdown(teamID int64, spanID, model string, startMs, endMs int64) (TokenBreakdownDTO, error) {
	row, err := s.repo.GetTokenBreakdown(context.Background(), teamID, spanID, model, startMs, endMs)
	if err != nil {
		return TokenBreakdownDTO{}, err
	}
	return TokenBreakdownDTO{
		InputTokens:    row.InputTokens,
		OutputTokens:   row.OutputTokens,
		TotalTokens:    row.InputTokens + row.OutputTokens,
		AvgInputModel:  row.AvgInputModel,
		AvgOutputModel: row.AvgOutputModel,
	}, nil
}
