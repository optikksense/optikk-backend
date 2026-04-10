package explorer

import "context"

// Service defines the business logic contract for the AI explorer module.
type Service interface {
	GetSpans(f ExplorerFilter) ([]SpanDTO, error)
	GetFacets(f ExplorerFilter) (FacetsResponseDTO, error)
	GetSummary(f ExplorerFilter) (ExplorerSummaryDTO, error)
	GetHistogram(f ExplorerFilter) ([]HistogramPointDTO, error)
}

type explorerService struct {
	repo Repository
}

// NewService creates a new explorer Service.
func NewService(repo Repository) Service {
	return &explorerService{repo: repo}
}

func (s *explorerService) GetSpans(f ExplorerFilter) ([]SpanDTO, error) {
	rows, err := s.repo.GetSpans(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]SpanDTO, len(rows))
	for i, r := range rows {
		result[i] = SpanDTO{
			SpanID:        r.SpanID,
			TraceID:       r.TraceID,
			ParentSpanID:  r.ParentSpanID,
			ServiceName:   r.ServiceName,
			OperationName: r.OperationName,
			Model:         r.Model,
			Provider:      r.Provider,
			OperationType: r.OperationType,
			Timestamp:     r.Timestamp,
			DurationMs:    r.DurationMs,
			InputTokens:   r.InputTokens,
			OutputTokens:  r.OutputTokens,
			TotalTokens:   r.InputTokens + r.OutputTokens,
			HasError:      r.HasError,
			StatusMessage: r.StatusMessage,
			FinishReason:  r.FinishReason,
			Temperature:   r.Temperature,
		}
	}
	return result, nil
}

func (s *explorerService) GetFacets(f ExplorerFilter) (FacetsResponseDTO, error) {
	ctx := context.Background()
	facets := map[string]string{
		"models":        "attributes.'gen_ai.request.model'::String",
		"providers":     "attributes.'gen_ai.system'::String",
		"operations":    "attributes.'gen_ai.operation.name'::String",
		"services":      "service_name",
		"finishReasons": "attributes.'gen_ai.response.finish_reasons'::String",
	}
	result := FacetsResponseDTO{}
	for key, expr := range facets {
		rows, err := s.repo.GetFacetValues(ctx, f, expr)
		if err != nil {
			return FacetsResponseDTO{}, err
		}
		vals := make([]FacetValueDTO, len(rows))
		for i, r := range rows {
			vals[i] = FacetValueDTO{Value: r.Value, Count: r.Count}
		}
		facet := FacetDTO{Values: vals}
		switch key {
		case "models":
			result.Models = facet
		case "providers":
			result.Providers = facet
		case "operations":
			result.Operations = facet
		case "services":
			result.Services = facet
		case "finishReasons":
			result.FinishReasons = facet
		}
	}
	return result, nil
}

func (s *explorerService) GetSummary(f ExplorerFilter) (ExplorerSummaryDTO, error) {
	row, err := s.repo.GetSummary(context.Background(), f)
	if err != nil {
		return ExplorerSummaryDTO{}, err
	}
	return ExplorerSummaryDTO{
		TotalSpans:   row.TotalSpans,
		ErrorCount:   row.ErrorCount,
		AvgLatencyMs: row.AvgLatencyMs,
		P95Ms:        row.P95Ms,
		TotalTokens:  row.TotalTokens,
		UniqueModels: row.UniqueModels,
	}, nil
}

func (s *explorerService) GetHistogram(f ExplorerFilter) ([]HistogramPointDTO, error) {
	rows, err := s.repo.GetHistogram(context.Background(), f)
	if err != nil {
		return nil, err
	}
	result := make([]HistogramPointDTO, len(rows))
	for i, r := range rows {
		result[i] = HistogramPointDTO{Timestamp: r.Timestamp, Count: r.Count}
	}
	return result, nil
}
