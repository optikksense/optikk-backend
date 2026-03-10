package tracedetail

// Service encapsulates business logic for trace detail endpoints.
type Service interface {
	GetSpanEvents(teamID int64, traceID string) ([]SpanEvent, error)
	GetSpanKindBreakdown(teamID int64, traceID string) ([]SpanKindDuration, error)
	GetCriticalPath(teamID int64, traceID string) ([]CriticalPathSpan, error)
	GetSpanSelfTimes(teamID int64, traceID string) ([]SpanSelfTime, error)
	GetErrorPath(teamID int64, traceID string) ([]ErrorPathSpan, error)
	GetSpanAttributes(teamID int64, traceID, spanID string) (*SpanAttributes, error)
	GetRelatedTraces(teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
}

// TraceDetailService implements Service.
type TraceDetailService struct {
	repo Repository
}

// NewService creates a new trace detail service.
func NewService(repo Repository) Service {
	return &TraceDetailService{repo: repo}
}

func (s *TraceDetailService) GetSpanEvents(teamID int64, traceID string) ([]SpanEvent, error) {
	return s.repo.GetSpanEvents(teamID, traceID)
}

func (s *TraceDetailService) GetSpanKindBreakdown(teamID int64, traceID string) ([]SpanKindDuration, error) {
	return s.repo.GetSpanKindBreakdown(teamID, traceID)
}

func (s *TraceDetailService) GetCriticalPath(teamID int64, traceID string) ([]CriticalPathSpan, error) {
	return s.repo.GetCriticalPath(teamID, traceID)
}

func (s *TraceDetailService) GetSpanSelfTimes(teamID int64, traceID string) ([]SpanSelfTime, error) {
	return s.repo.GetSpanSelfTimes(teamID, traceID)
}

func (s *TraceDetailService) GetErrorPath(teamID int64, traceID string) ([]ErrorPathSpan, error) {
	return s.repo.GetErrorPath(teamID, traceID)
}

func (s *TraceDetailService) GetSpanAttributes(teamID int64, traceID, spanID string) (*SpanAttributes, error) {
	return s.repo.GetSpanAttributes(teamID, traceID, spanID)
}

func (s *TraceDetailService) GetRelatedTraces(teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	return s.repo.GetRelatedTraces(teamID, serviceName, operationName, startMs, endMs, excludeTraceID, limit)
}
