package runs

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) ListRuns(ctx context.Context, f LLMRunFilters) ([]LLMRun, error) {
	rows, err := s.repo.ListRuns(ctx, f)
	if err != nil {
		return nil, err
	}
	return mapLLMRunDTOs(rows), nil
}

func (s *Service) GetRunsSummary(ctx context.Context, f LLMRunFilters) (*LLMRunSummary, error) {
	row, err := s.repo.GetRunsSummary(ctx, f)
	if err != nil || row == nil {
		return nil, err
	}
	model := LLMRunSummary(*row)
	return &model, nil
}

func (s *Service) ListModels(ctx context.Context, f LLMRunFilters) ([]LLMRunModel, error) {
	rows, err := s.repo.ListModels(ctx, f)
	if err != nil {
		return nil, err
	}
	return mapLLMRunModelDTOs(rows), nil
}

func (s *Service) ListOperations(ctx context.Context, f LLMRunFilters) ([]LLMRunOperation, error) {
	rows, err := s.repo.ListOperations(ctx, f)
	if err != nil {
		return nil, err
	}
	return mapLLMRunOperationDTOs(rows), nil
}
