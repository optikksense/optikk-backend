package redmetrics

// Service encapsulates business logic for RED metrics endpoints.
type Service interface {
	GetTopSlowOperations(teamUUID string, startMs, endMs int64, limit int) ([]SlowOperation, error)
	GetTopErrorOperations(teamUUID string, startMs, endMs int64, limit int) ([]ErrorOperation, error)
	GetHTTPStatusDistribution(teamUUID string, startMs, endMs int64) ([]HTTPStatusBucket, []HTTPStatusTimePoint, error)
	GetServiceScorecard(teamUUID string, startMs, endMs int64) ([]ServiceScorecard, error)
	GetApdex(teamUUID string, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error)
}

// REDMetricsService implements Service.
type REDMetricsService struct {
	repo Repository
}

// NewService creates a new RED metrics service.
func NewService(repo Repository) Service {
	return &REDMetricsService{repo: repo}
}

func (s *REDMetricsService) GetTopSlowOperations(teamUUID string, startMs, endMs int64, limit int) ([]SlowOperation, error) {
	return s.repo.GetTopSlowOperations(teamUUID, startMs, endMs, limit)
}

func (s *REDMetricsService) GetTopErrorOperations(teamUUID string, startMs, endMs int64, limit int) ([]ErrorOperation, error) {
	return s.repo.GetTopErrorOperations(teamUUID, startMs, endMs, limit)
}

func (s *REDMetricsService) GetHTTPStatusDistribution(teamUUID string, startMs, endMs int64) ([]HTTPStatusBucket, []HTTPStatusTimePoint, error) {
	return s.repo.GetHTTPStatusDistribution(teamUUID, startMs, endMs)
}

func (s *REDMetricsService) GetServiceScorecard(teamUUID string, startMs, endMs int64) ([]ServiceScorecard, error) {
	return s.repo.GetServiceScorecard(teamUUID, startMs, endMs)
}

func (s *REDMetricsService) GetApdex(teamUUID string, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error) {
	return s.repo.GetApdex(teamUUID, startMs, endMs, satisfiedMs, toleratingMs)
}
