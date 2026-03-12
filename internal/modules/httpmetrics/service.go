package httpmetrics

type Service interface {
	GetRequestRate(teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error)
	GetRequestDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetActiveRequests(teamID int64, startMs, endMs int64) ([]TimeBucket, error)
	GetRequestBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetResponseBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetClientDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetDNSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetTLSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
}

type HTTPMetricsService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &HTTPMetricsService{repo: repo}
}

func (s *HTTPMetricsService) GetRequestRate(teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
	return s.repo.GetRequestRate(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRequestDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRequestDuration(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetActiveRequests(teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetActiveRequests(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRequestBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRequestBodySize(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetResponseBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetResponseBodySize(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetClientDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetClientDuration(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetDNSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetDNSDuration(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTLSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetTLSDuration(teamID, startMs, endMs)
}
