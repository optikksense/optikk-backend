package errorfingerprint

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) ListFingerprints(teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorFingerprint, error) {
	return s.repo.ListFingerprints(teamID, startMs, endMs, serviceName, limit)
}

func (s *Service) GetFingerprintTrend(teamID int64, startMs, endMs int64, serviceName, operationName, exceptionType, statusMessage string) ([]FingerprintTrendPoint, error) {
	return s.repo.GetFingerprintTrend(teamID, startMs, endMs, serviceName, operationName, exceptionType, statusMessage)
}
