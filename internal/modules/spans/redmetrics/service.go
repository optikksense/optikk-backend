package redmetrics

// Service encapsulates business logic for RED metrics endpoints.
type Service interface {
	GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error)
	GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error)
	GetHTTPStatusDistribution(teamID int64, startMs, endMs int64) ([]HTTPStatusBucket, []HTTPStatusTimePoint, error)
	GetServiceScorecard(teamID int64, startMs, endMs int64) ([]ServiceScorecard, error)
	GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error)
}

// REDMetricsService implements Service.
type REDMetricsService struct {
	repo Repository
}

// NewService creates a new RED metrics service.
func NewService(repo Repository) Service {
	return &REDMetricsService{repo: repo}
}

func (s *REDMetricsService) GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error) {
	return s.repo.GetTopSlowOperations(teamID, startMs, endMs, limit)
}

func (s *REDMetricsService) GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error) {
	return s.repo.GetTopErrorOperations(teamID, startMs, endMs, limit)
}

func (s *REDMetricsService) GetHTTPStatusDistribution(teamID int64, startMs, endMs int64) ([]HTTPStatusBucket, []HTTPStatusTimePoint, error) {
	return s.repo.GetHTTPStatusDistribution(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetServiceScorecard(teamID int64, startMs, endMs int64) ([]ServiceScorecard, error) {
	return s.repo.GetServiceScorecard(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error) {
	return s.repo.GetApdex(teamID, startMs, endMs, satisfiedMs, toleratingMs)
}
