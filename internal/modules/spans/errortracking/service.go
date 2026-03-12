package errortracking

type Service interface {
	GetExceptionRateByType(teamID int64, startMs, endMs int64, serviceName string) ([]ExceptionRatePoint, error)
	GetErrorHotspot(teamID int64, startMs, endMs int64) ([]ErrorHotspotCell, error)
	GetHTTP5xxByRoute(teamID int64, startMs, endMs int64, serviceName string) ([]HTTP5xxByRoute, error)
}

type ErrorTrackingService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &ErrorTrackingService{repo: repo}
}

func (s *ErrorTrackingService) GetExceptionRateByType(teamID int64, startMs, endMs int64, serviceName string) ([]ExceptionRatePoint, error) {
	return s.repo.GetExceptionRateByType(teamID, startMs, endMs, serviceName)
}

func (s *ErrorTrackingService) GetErrorHotspot(teamID int64, startMs, endMs int64) ([]ErrorHotspotCell, error) {
	return s.repo.GetErrorHotspot(teamID, startMs, endMs)
}

func (s *ErrorTrackingService) GetHTTP5xxByRoute(teamID int64, startMs, endMs int64, serviceName string) ([]HTTP5xxByRoute, error) {
	return s.repo.GetHTTP5xxByRoute(teamID, startMs, endMs, serviceName)
}
