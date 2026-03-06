package errortracking

// Service encapsulates business logic for error tracking endpoints.
type Service interface {
	GetExceptionRateByType(teamUUID string, startMs, endMs int64, serviceName string) ([]ExceptionRatePoint, error)
	GetErrorHotspot(teamUUID string, startMs, endMs int64) ([]ErrorHotspotCell, error)
	GetHTTP5xxByRoute(teamUUID string, startMs, endMs int64, serviceName string) ([]HTTP5xxByRoute, error)
}

// ErrorTrackingService implements Service.
type ErrorTrackingService struct {
	repo Repository
}

// NewService creates a new error tracking service.
func NewService(repo Repository) Service {
	return &ErrorTrackingService{repo: repo}
}

func (s *ErrorTrackingService) GetExceptionRateByType(teamUUID string, startMs, endMs int64, serviceName string) ([]ExceptionRatePoint, error) {
	return s.repo.GetExceptionRateByType(teamUUID, startMs, endMs, serviceName)
}

func (s *ErrorTrackingService) GetErrorHotspot(teamUUID string, startMs, endMs int64) ([]ErrorHotspotCell, error) {
	return s.repo.GetErrorHotspot(teamUUID, startMs, endMs)
}

func (s *ErrorTrackingService) GetHTTP5xxByRoute(teamUUID string, startMs, endMs int64, serviceName string) ([]HTTP5xxByRoute, error) {
	return s.repo.GetHTTP5xxByRoute(teamUUID, startMs, endMs, serviceName)
}
