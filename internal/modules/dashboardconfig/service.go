package dashboardconfig

// Service encapsulates the business logic for dashboard configurations.
type Service interface {
	EnsureTable() error
	GetConfig(teamID int64, pageID string) (string, error)
	SaveConfig(teamID int64, pageID, configYaml string) error
}

// DashboardConfigService provides business logic orchestration for dashboard configurations.
type DashboardConfigService struct {
	repo Repository
}

// NewService creates a new DashboardConfigService.
func NewService(repo Repository) *DashboardConfigService {
	return &DashboardConfigService{repo: repo}
}

func (s *DashboardConfigService) EnsureTable() error {
	return s.repo.EnsureTable()
}

func (s *DashboardConfigService) GetConfig(teamID int64, pageID string) (string, error) {
	return s.repo.GetConfig(teamID, pageID)
}

func (s *DashboardConfigService) SaveConfig(teamID int64, pageID, configYaml string) error {
	return s.repo.SaveConfig(teamID, pageID, configYaml)
}
