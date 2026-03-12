package database

type Service interface {
	GetSummaryStats(teamID int64, startMs, endMs int64, f Filters) (SummaryStats, error)
	GetDetectedSystems(teamID int64, startMs, endMs int64) ([]DetectedSystem, error)
	GetLatencyBySystem(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyByOperation(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyByCollection(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyByNamespace(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyByServer(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyHeatmap(teamID int64, startMs, endMs int64, f Filters) ([]LatencyHeatmapBucket, error)
	GetOpsBySystem(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error)
	GetOpsByOperation(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error)
	GetOpsByCollection(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error)
	GetReadVsWrite(teamID int64, startMs, endMs int64, f Filters) ([]ReadWritePoint, error)
	GetOpsByNamespace(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error)
	GetSlowQueryPatterns(teamID int64, startMs, endMs int64, f Filters, limit int) ([]SlowQueryPattern, error)
	GetSlowestCollections(teamID int64, startMs, endMs int64, f Filters) ([]SlowCollectionRow, error)
	GetSlowQueryRate(teamID int64, startMs, endMs int64, f Filters, thresholdMs float64) ([]SlowRatePoint, error)
	GetP99ByQueryText(teamID int64, startMs, endMs int64, f Filters, limit int) ([]P99ByQueryText, error)
	GetErrorsBySystem(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorsByOperation(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorsByErrorType(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorsByCollection(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorsByResponseStatus(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorRatio(teamID int64, startMs, endMs int64, f Filters) ([]ErrorRatioPoint, error)
	GetConnectionCountSeries(teamID int64, startMs, endMs int64) ([]ConnectionCountPoint, error)
	GetConnectionUtilization(teamID int64, startMs, endMs int64) ([]ConnectionUtilPoint, error)
	GetConnectionLimits(teamID int64, startMs, endMs int64) ([]ConnectionLimits, error)
	GetPendingRequests(teamID int64, startMs, endMs int64) ([]PendingRequestsPoint, error)
	GetConnectionTimeoutRate(teamID int64, startMs, endMs int64) ([]ConnectionTimeoutPoint, error)
	GetConnectionWaitTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error)
	GetConnectionCreateTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error)
	GetConnectionUseTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error)
	GetCollectionLatency(teamID int64, startMs, endMs int64, collection string, f Filters) ([]LatencyTimeSeries, error)
	GetCollectionOps(teamID int64, startMs, endMs int64, collection string, f Filters) ([]OpsTimeSeries, error)
	GetCollectionErrors(teamID int64, startMs, endMs int64, collection string, f Filters) ([]ErrorTimeSeries, error)
	GetCollectionQueryTexts(teamID int64, startMs, endMs int64, collection string, f Filters, limit int) ([]CollectionTopQuery, error)
	GetCollectionReadVsWrite(teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error)
	GetSystemLatency(teamID int64, startMs, endMs int64, dbSystem string, f Filters) ([]LatencyTimeSeries, error)
	GetSystemOps(teamID int64, startMs, endMs int64, dbSystem string, f Filters) ([]OpsTimeSeries, error)
	GetSystemTopCollectionsByLatency(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error)
	GetSystemTopCollectionsByVolume(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error)
	GetSystemErrors(teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error)
	GetSystemNamespaces(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error)
}

type DatabaseService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &DatabaseService{repo: repo}
}

func (s *DatabaseService) GetSummaryStats(teamID int64, startMs, endMs int64, f Filters) (SummaryStats, error) {
	return s.repo.GetSummaryStats(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetDetectedSystems(teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	return s.repo.GetDetectedSystems(teamID, startMs, endMs)
}
func (s *DatabaseService) GetLatencyBySystem(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyBySystem(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetLatencyByOperation(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByOperation(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetLatencyByCollection(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByCollection(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetLatencyByNamespace(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByNamespace(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetLatencyByServer(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByServer(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetLatencyHeatmap(teamID int64, startMs, endMs int64, f Filters) ([]LatencyHeatmapBucket, error) {
	return s.repo.GetLatencyHeatmap(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetOpsBySystem(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetOpsBySystem(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetOpsByOperation(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetOpsByOperation(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetOpsByCollection(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetOpsByCollection(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetReadVsWrite(teamID int64, startMs, endMs int64, f Filters) ([]ReadWritePoint, error) {
	return s.repo.GetReadVsWrite(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetOpsByNamespace(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetOpsByNamespace(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetSlowQueryPatterns(teamID int64, startMs, endMs int64, f Filters, limit int) ([]SlowQueryPattern, error) {
	return s.repo.GetSlowQueryPatterns(teamID, startMs, endMs, f, limit)
}
func (s *DatabaseService) GetSlowestCollections(teamID int64, startMs, endMs int64, f Filters) ([]SlowCollectionRow, error) {
	return s.repo.GetSlowestCollections(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetSlowQueryRate(teamID int64, startMs, endMs int64, f Filters, thresholdMs float64) ([]SlowRatePoint, error) {
	return s.repo.GetSlowQueryRate(teamID, startMs, endMs, f, thresholdMs)
}
func (s *DatabaseService) GetP99ByQueryText(teamID int64, startMs, endMs int64, f Filters, limit int) ([]P99ByQueryText, error) {
	return s.repo.GetP99ByQueryText(teamID, startMs, endMs, f, limit)
}
func (s *DatabaseService) GetErrorsBySystem(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsBySystem(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetErrorsByOperation(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsByOperation(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetErrorsByErrorType(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsByErrorType(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetErrorsByCollection(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsByCollection(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetErrorsByResponseStatus(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsByResponseStatus(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetErrorRatio(teamID int64, startMs, endMs int64, f Filters) ([]ErrorRatioPoint, error) {
	return s.repo.GetErrorRatio(teamID, startMs, endMs, f)
}
func (s *DatabaseService) GetConnectionCountSeries(teamID int64, startMs, endMs int64) ([]ConnectionCountPoint, error) {
	return s.repo.GetConnectionCountSeries(teamID, startMs, endMs)
}
func (s *DatabaseService) GetConnectionUtilization(teamID int64, startMs, endMs int64) ([]ConnectionUtilPoint, error) {
	return s.repo.GetConnectionUtilization(teamID, startMs, endMs)
}
func (s *DatabaseService) GetConnectionLimits(teamID int64, startMs, endMs int64) ([]ConnectionLimits, error) {
	return s.repo.GetConnectionLimits(teamID, startMs, endMs)
}
func (s *DatabaseService) GetPendingRequests(teamID int64, startMs, endMs int64) ([]PendingRequestsPoint, error) {
	return s.repo.GetPendingRequests(teamID, startMs, endMs)
}
func (s *DatabaseService) GetConnectionTimeoutRate(teamID int64, startMs, endMs int64) ([]ConnectionTimeoutPoint, error) {
	return s.repo.GetConnectionTimeoutRate(teamID, startMs, endMs)
}
func (s *DatabaseService) GetConnectionWaitTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error) {
	return s.repo.GetConnectionWaitTime(teamID, startMs, endMs)
}
func (s *DatabaseService) GetConnectionCreateTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error) {
	return s.repo.GetConnectionCreateTime(teamID, startMs, endMs)
}
func (s *DatabaseService) GetConnectionUseTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error) {
	return s.repo.GetConnectionUseTime(teamID, startMs, endMs)
}
func (s *DatabaseService) GetCollectionLatency(teamID int64, startMs, endMs int64, collection string, f Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetCollectionLatency(teamID, startMs, endMs, collection, f)
}
func (s *DatabaseService) GetCollectionOps(teamID int64, startMs, endMs int64, collection string, f Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetCollectionOps(teamID, startMs, endMs, collection, f)
}
func (s *DatabaseService) GetCollectionErrors(teamID int64, startMs, endMs int64, collection string, f Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetCollectionErrors(teamID, startMs, endMs, collection, f)
}
func (s *DatabaseService) GetCollectionQueryTexts(teamID int64, startMs, endMs int64, collection string, f Filters, limit int) ([]CollectionTopQuery, error) {
	return s.repo.GetCollectionQueryTexts(teamID, startMs, endMs, collection, f, limit)
}
func (s *DatabaseService) GetCollectionReadVsWrite(teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error) {
	return s.repo.GetCollectionReadVsWrite(teamID, startMs, endMs, collection)
}
func (s *DatabaseService) GetSystemLatency(teamID int64, startMs, endMs int64, dbSystem string, f Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetSystemLatency(teamID, startMs, endMs, dbSystem, f)
}
func (s *DatabaseService) GetSystemOps(teamID int64, startMs, endMs int64, dbSystem string, f Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetSystemOps(teamID, startMs, endMs, dbSystem, f)
}
func (s *DatabaseService) GetSystemTopCollectionsByLatency(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	return s.repo.GetSystemTopCollectionsByLatency(teamID, startMs, endMs, dbSystem)
}
func (s *DatabaseService) GetSystemTopCollectionsByVolume(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	return s.repo.GetSystemTopCollectionsByVolume(teamID, startMs, endMs, dbSystem)
}
func (s *DatabaseService) GetSystemErrors(teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error) {
	return s.repo.GetSystemErrors(teamID, startMs, endMs, dbSystem)
}
func (s *DatabaseService) GetSystemNamespaces(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	return s.repo.GetSystemNamespaces(teamID, startMs, endMs, dbSystem)
}
