package runs

type Service interface {
	ListRuns(f LLMRunFilters) ([]LLMRun, error)
	GetRunsSummary(f LLMRunFilters) (*LLMRunSummary, error)
	ListModels(f LLMRunFilters) ([]LLMRunModel, error)
	ListOperations(f LLMRunFilters) ([]LLMRunOperation, error)
}

type RunsService struct{ repo Repository }

func NewService(repo Repository) *RunsService { return &RunsService{repo: repo} }

func (s *RunsService) ListRuns(f LLMRunFilters) ([]LLMRun, error) {
	return s.repo.ListRuns(f)
}

func (s *RunsService) GetRunsSummary(f LLMRunFilters) (*LLMRunSummary, error) {
	return s.repo.GetRunsSummary(f)
}

func (s *RunsService) ListModels(f LLMRunFilters) ([]LLMRunModel, error) {
	return s.repo.ListModels(f)
}

func (s *RunsService) ListOperations(f LLMRunFilters) ([]LLMRunOperation, error) {
	return s.repo.ListOperations(f)
}
