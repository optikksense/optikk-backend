package httpmetrics

// Service encapsulates the business logic for HTTP metrics.
type Service interface {
	GetRequestRate(teamUUID string, startMs, endMs int64) ([]StatusCodeBucket, error)
	GetRequestDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetActiveRequests(teamUUID string, startMs, endMs int64) ([]TimeBucket, error)
	GetRequestBodySize(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetResponseBodySize(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetClientDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetDNSDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
	GetTLSDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error)
}

// HTTPMetricsService implements Service.
type HTTPMetricsService struct {
	repo Repository
}

// NewService creates a new HTTPMetricsService.
func NewService(repo Repository) Service {
	return &HTTPMetricsService{repo: repo}
}

func (s *HTTPMetricsService) GetRequestRate(teamUUID string, startMs, endMs int64) ([]StatusCodeBucket, error) {
	return s.repo.GetRequestRate(teamUUID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRequestDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRequestDuration(teamUUID, startMs, endMs)
}

func (s *HTTPMetricsService) GetActiveRequests(teamUUID string, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetActiveRequests(teamUUID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRequestBodySize(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRequestBodySize(teamUUID, startMs, endMs)
}

func (s *HTTPMetricsService) GetResponseBodySize(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetResponseBodySize(teamUUID, startMs, endMs)
}

func (s *HTTPMetricsService) GetClientDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetClientDuration(teamUUID, startMs, endMs)
}

func (s *HTTPMetricsService) GetDNSDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetDNSDuration(teamUUID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTLSDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetTLSDuration(teamUUID, startMs, endMs)
}
