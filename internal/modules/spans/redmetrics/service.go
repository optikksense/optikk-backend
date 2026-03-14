package redmetrics

type Service interface {
	GetSummary(teamID int64, startMs, endMs int64) (REDSummary, error)
	GetServiceScorecard(teamID int64, startMs, endMs int64) ([]ServiceScorecard, error)
	GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error)
	GetHTTPStatusDistribution(teamID int64, startMs, endMs int64) ([]HTTPStatusBucket, error)
	GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error)
	GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error)
	GetRequestRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error)
	GetErrorRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error)
	GetP95LatencyTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error)
	GetSpanKindBreakdown(teamID int64, startMs, endMs int64) ([]SpanKindPoint, error)
	GetErrorsByRoute(teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error)
	GetLatencyBreakdown(teamID int64, startMs, endMs int64) ([]LatencyBreakdown, error)
}

type REDMetricsService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &REDMetricsService{repo: repo}
}

func (s *REDMetricsService) GetSummary(teamID int64, startMs, endMs int64) (REDSummary, error) {
	return s.repo.GetSummary(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetServiceScorecard(teamID int64, startMs, endMs int64) ([]ServiceScorecard, error) {
	return s.repo.GetServiceScorecard(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error) {
	return s.repo.GetApdex(teamID, startMs, endMs, satisfiedMs, toleratingMs)
}

func (s *REDMetricsService) GetHTTPStatusDistribution(teamID int64, startMs, endMs int64) ([]HTTPStatusBucket, error) {
	return s.repo.GetHTTPStatusDistribution(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error) {
	return s.repo.GetTopSlowOperations(teamID, startMs, endMs, limit)
}

func (s *REDMetricsService) GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error) {
	return s.repo.GetTopErrorOperations(teamID, startMs, endMs, limit)
}

func (s *REDMetricsService) GetRequestRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	return s.repo.GetRequestRateTimeSeries(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetErrorRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	return s.repo.GetErrorRateTimeSeries(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetP95LatencyTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error) {
	return s.repo.GetP95LatencyTimeSeries(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetSpanKindBreakdown(teamID int64, startMs, endMs int64) ([]SpanKindPoint, error) {
	return s.repo.GetSpanKindBreakdown(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetErrorsByRoute(teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error) {
	return s.repo.GetErrorsByRoute(teamID, startMs, endMs)
}

func (s *REDMetricsService) GetLatencyBreakdown(teamID int64, startMs, endMs int64) ([]LatencyBreakdown, error) {
	return s.repo.GetLatencyBreakdown(teamID, startMs, endMs)
}
