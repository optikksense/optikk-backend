package tracedetail

// Service encapsulates business logic for trace detail endpoints.
type Service interface {
	GetSpanEvents(teamUUID, traceID string) ([]SpanEvent, error)
	GetSpanKindBreakdown(teamUUID, traceID string) ([]SpanKindDuration, error)
	GetCriticalPath(teamUUID, traceID string) ([]CriticalPathSpan, error)
	GetSpanSelfTimes(teamUUID, traceID string) ([]SpanSelfTime, error)
	GetErrorPath(teamUUID, traceID string) ([]ErrorPathSpan, error)
	GetSpanAttributes(teamUUID, traceID, spanID string) (*SpanAttributes, error)
	GetRelatedTraces(teamUUID, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
}

// TraceDetailService implements Service.
type TraceDetailService struct {
	repo Repository
}

// NewService creates a new trace detail service.
func NewService(repo Repository) Service {
	return &TraceDetailService{repo: repo}
}

func (s *TraceDetailService) GetSpanEvents(teamUUID, traceID string) ([]SpanEvent, error) {
	return s.repo.GetSpanEvents(teamUUID, traceID)
}

func (s *TraceDetailService) GetSpanKindBreakdown(teamUUID, traceID string) ([]SpanKindDuration, error) {
	return s.repo.GetSpanKindBreakdown(teamUUID, traceID)
}

func (s *TraceDetailService) GetCriticalPath(teamUUID, traceID string) ([]CriticalPathSpan, error) {
	return s.repo.GetCriticalPath(teamUUID, traceID)
}

func (s *TraceDetailService) GetSpanSelfTimes(teamUUID, traceID string) ([]SpanSelfTime, error) {
	return s.repo.GetSpanSelfTimes(teamUUID, traceID)
}

func (s *TraceDetailService) GetErrorPath(teamUUID, traceID string) ([]ErrorPathSpan, error) {
	return s.repo.GetErrorPath(teamUUID, traceID)
}

func (s *TraceDetailService) GetSpanAttributes(teamUUID, traceID, spanID string) (*SpanAttributes, error) {
	return s.repo.GetSpanAttributes(teamUUID, traceID, spanID)
}

func (s *TraceDetailService) GetRelatedTraces(teamUUID, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	return s.repo.GetRelatedTraces(teamUUID, serviceName, operationName, startMs, endMs, excludeTraceID, limit)
}
