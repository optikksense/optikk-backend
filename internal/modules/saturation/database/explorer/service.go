package explorer

import (
	"context"
	"sort"
	"strings"

	dbconnections "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/connections"
	dberrors "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/errors"
	dbfilter "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
	dblatency "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/latency"
	dbslowqueries "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/slowqueries"
	dbsummary "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/summary"
	dbsystem "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/system"
	dbsystems "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/systems"
	dbvolume "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/volume"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	dbSummary     *dbsummary.Service
	dbSystems     *dbsystems.Service
	dbSystem      *dbsystem.Service
	dbLatency     *dblatency.Service
	dbVolume      *dbvolume.Service
	dbErrors      *dberrors.Service
	dbSlowQueries *dbslowqueries.Service
	dbConnections *dbconnections.Service
}

func NewService(
	dbSummarySvc *dbsummary.Service,
	dbSystemsSvc *dbsystems.Service,
	dbSystemSvc *dbsystem.Service,
	dbLatencySvc *dblatency.Service,
	dbVolumeSvc *dbvolume.Service,
	dbErrorsSvc *dberrors.Service,
	dbSlowSvc *dbslowqueries.Service,
	dbConnectionsSvc *dbconnections.Service,
) *Service {
	return &Service{
		dbSummary:     dbSummarySvc,
		dbSystems:     dbSystemsSvc,
		dbSystem:      dbSystemSvc,
		dbLatency:     dbLatencySvc,
		dbVolume:      dbVolumeSvc,
		dbErrors:      dbErrorsSvc,
		dbSlowQueries: dbSlowSvc,
		dbConnections: dbConnectionsSvc,
	}
}

func (s *Service) GetDatastoreSummary(ctx context.Context, teamID int64, startMs, endMs int64) (DatastoreSummaryResponse, error) {
	var (
		systemRows []dbsystems.DetectedSystem
		summary    dbsummary.SummaryStats
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		systemRows, err = s.dbSystems.GetDetectedSystems(gctx, teamID, startMs, endMs)
		return err
	})
	g.Go(func() error {
		var err error
		summary, err = s.dbSummary.GetSummaryStats(gctx, teamID, startMs, endMs, dbfilter.Filters{})
		return err
	})
	if err := g.Wait(); err != nil {
		return DatastoreSummaryResponse{}, err
	}

	totalErrors := int64(0)
	databaseSystems := 0
	redisSystems := 0
	for _, row := range systemRows {
		totalErrors += row.ErrorCount
		if isRedisSystem(row.DBSystem) {
			redisSystems++
			continue
		}
		databaseSystems++
	}

	return DatastoreSummaryResponse{
		TotalSystems:      len(systemRows),
		DatabaseSystems:   databaseSystems,
		RedisSystems:      redisSystems,
		QueryCount:        summary.SpanCount,
		P95LatencyMs:      derefFloat(summary.P95LatencyMs),
		ErrorRate:         safeRatioPct(totalErrors, summary.SpanCount),
		ActiveConnections: summary.ActiveConnections,
	}, nil
}

func (s *Service) GetDatastoreSystems(ctx context.Context, teamID int64, startMs, endMs int64) ([]DatastoreSystemRow, error) {
	systemRows, err := s.dbSystems.GetSystemSummaries(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	rows := make([]DatastoreSystemRow, 0, len(systemRows))
	for _, detected := range systemRows {
		rows = append(rows, DatastoreSystemRow{
			System:            detected.DBSystem,
			Category:          datastoreCategory(detected.DBSystem),
			QueryCount:        detected.QueryCount,
			AvgLatencyMs:      detected.AvgLatencyMs,
			P95LatencyMs:      detected.P95LatencyMs,
			ErrorRate:         safeRatioPct(detected.ErrorCount, detected.QueryCount),
			ActiveConnections: detected.ActiveConnections,
			ServerHint:        detected.ServerAddress,
			LastSeen:          detected.LastSeen,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].QueryCount == rows[j].QueryCount {
			return rows[i].System < rows[j].System
		}
		return rows[i].QueryCount > rows[j].QueryCount
	})

	return rows, nil
}

func (s *Service) GetDatastoreSystemOverview(ctx context.Context, teamID int64, startMs, endMs int64, system string) (DatastoreSystemOverview, error) {
	var (
		summary        dbsummary.SummaryStats
		systems        []dbsystems.DetectedSystem
		namespaces     []dbsystem.SystemNamespace
		topCollections []dbsystem.SystemCollectionRow
		readWrite      []dbvolume.ReadWritePoint
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		summary, err = s.dbSummary.GetSummaryStats(gctx, teamID, startMs, endMs, dbfilter.Filters{
			DBSystem: []string{system},
		})
		return err
	})
	g.Go(func() error {
		var err error
		systems, err = s.dbSystems.GetDetectedSystems(gctx, teamID, startMs, endMs)
		return err
	})
	g.Go(func() error {
		var err error
		namespaces, err = s.dbSystem.GetSystemNamespaces(gctx, teamID, startMs, endMs, system)
		return err
	})
	g.Go(func() error {
		var err error
		topCollections, err = s.dbSystem.GetSystemTopCollectionsByLatency(gctx, teamID, startMs, endMs, system)
		return err
	})
	g.Go(func() error {
		var err error
		readWrite, err = s.dbVolume.GetReadVsWrite(gctx, teamID, startMs, endMs, dbfilter.Filters{
			DBSystem: []string{system},
		})
		return err
	})
	if err := g.Wait(); err != nil {
		return DatastoreSystemOverview{}, err
	}

	var detected *dbsystems.DetectedSystem
	for i := range systems {
		if strings.EqualFold(systems[i].DBSystem, system) {
			detected = &systems[i]
			break
		}
	}

	spotlights := make([]DatastoreCollectionSpotlight, 0, len(topCollections))
	for _, collection := range topCollections {
		spotlights = append(spotlights, DatastoreCollectionSpotlight{
			CollectionName: collection.CollectionName,
			P99Ms:          derefFloat(collection.P99Ms),
			OpsPerSec:      derefFloat(collection.OpsPerSec),
		})
	}

	return DatastoreSystemOverview{
		System:            system,
		Category:          datastoreCategory(system),
		QueryCount:        summary.SpanCount,
		ErrorRate:         safeRatioPct(errorCountFromDetected(detected), summary.SpanCount),
		AvgLatencyMs:      derefFloat(summary.AvgLatencyMs),
		P95LatencyMs:      derefFloat(summary.P95LatencyMs),
		P99LatencyMs:      derefFloat(summary.P99LatencyMs),
		ActiveConnections: summary.ActiveConnections,
		CacheHitRate:      summary.CacheHitRate,
		TopServer:         serverHintFromDetected(detected),
		NamespaceCount:    len(namespaces),
		CollectionCount:   len(spotlights),
		ReadOpsPerSec:     latestReadOps(readWrite),
		WriteOpsPerSec:    latestWriteOps(readWrite),
		TopCollections:    spotlights,
	}, nil
}

func (s *Service) GetDatastoreSystemServers(ctx context.Context, teamID int64, startMs, endMs int64, system string) ([]DatastoreServerRow, error) {
	rows, err := s.dbLatency.GetLatencyByServer(ctx, teamID, startMs, endMs, dbfilter.Filters{
		DBSystem: []string{system},
	})
	if err != nil {
		return nil, err
	}

	aggregated := make(map[string]DatastoreServerRow)
	latestTs := make(map[string]string)
	for _, row := range rows {
		server := strings.TrimSpace(row.GroupBy)
		if server == "" {
			continue
		}
		if row.TimeBucket >= latestTs[server] {
			latestTs[server] = row.TimeBucket
			aggregated[server] = DatastoreServerRow{
				Server: server,
				P50Ms:  derefFloat(row.P50Ms),
				P95Ms:  derefFloat(row.P95Ms),
				P99Ms:  derefFloat(row.P99Ms),
			}
		}
	}

	result := mapsToSortedRows(aggregated, func(left, right DatastoreServerRow) bool {
		if left.P95Ms == right.P95Ms {
			return left.Server < right.Server
		}
		return left.P95Ms > right.P95Ms
	})
	return result, nil
}

func (s *Service) GetDatastoreSystemNamespaces(ctx context.Context, teamID int64, startMs, endMs int64, system string) ([]DatastoreNamespaceRow, error) {
	rows, err := s.dbSystem.GetSystemNamespaces(ctx, teamID, startMs, endMs, system)
	if err != nil {
		return nil, err
	}

	result := make([]DatastoreNamespaceRow, 0, len(rows))
	for _, row := range rows {
		result = append(result, DatastoreNamespaceRow{
			Namespace: row.Namespace,
			SpanCount: row.SpanCount,
		})
	}
	return result, nil
}

func (s *Service) GetDatastoreSystemOperations(ctx context.Context, teamID int64, startMs, endMs int64, system string) ([]DatastoreOperationRow, error) {
	var (
		latencyRows []dbsystem.LatencyTimeSeries
		opsRows     []dbsystem.OpsTimeSeries
		errorRows   []dbsystem.ErrorTimeSeries
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		latencyRows, err = s.dbSystem.GetSystemLatency(gctx, teamID, startMs, endMs, system, dbfilter.Filters{})
		return err
	})
	g.Go(func() error {
		var err error
		opsRows, err = s.dbSystem.GetSystemOps(gctx, teamID, startMs, endMs, system, dbfilter.Filters{})
		return err
	})
	g.Go(func() error {
		var err error
		errorRows, err = s.dbSystem.GetSystemErrors(gctx, teamID, startMs, endMs, system)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	type opAggregate struct {
		DatastoreOperationRow
		latencyTs string
		opsTs     string
		errTs     string
	}

	aggregated := map[string]opAggregate{}

	for _, row := range latencyRows {
		key := strings.TrimSpace(row.GroupBy)
		if key == "" {
			continue
		}
		current := aggregated[key]
		if row.TimeBucket >= current.latencyTs {
			current.latencyTs = row.TimeBucket
			current.Operation = key
			current.P50Ms = derefFloat(row.P50Ms)
			current.P95Ms = derefFloat(row.P95Ms)
			current.P99Ms = derefFloat(row.P99Ms)
		}
		aggregated[key] = current
	}

	for _, row := range opsRows {
		key := strings.TrimSpace(row.GroupBy)
		if key == "" {
			continue
		}
		current := aggregated[key]
		if row.TimeBucket >= current.opsTs {
			current.opsTs = row.TimeBucket
			current.Operation = key
			current.OpsPerSec = derefFloat(row.OpsPerSec)
		}
		aggregated[key] = current
	}

	for _, row := range errorRows {
		key := strings.TrimSpace(row.GroupBy)
		if key == "" {
			continue
		}
		current := aggregated[key]
		if row.TimeBucket >= current.errTs {
			current.errTs = row.TimeBucket
			current.Operation = key
			current.ErrorsPerSec = derefFloat(row.ErrorsPerSec)
		}
		aggregated[key] = current
	}

	result := make([]DatastoreOperationRow, 0, len(aggregated))
	for _, row := range aggregated {
		result = append(result, row.DatastoreOperationRow)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].P95Ms == result[j].P95Ms {
			return result[i].Operation < result[j].Operation
		}
		return result[i].P95Ms > result[j].P95Ms
	})

	return result, nil
}

func (s *Service) GetDatastoreSystemErrors(ctx context.Context, teamID int64, startMs, endMs int64, system string) ([]DatastoreErrorRow, error) {
	rows, err := s.dbErrors.GetErrorsByErrorType(ctx, teamID, startMs, endMs, dbfilter.Filters{
		DBSystem: []string{system},
	})
	if err != nil {
		return nil, err
	}

	aggregated := map[string]DatastoreErrorRow{}
	latestTs := map[string]string{}
	for _, row := range rows {
		key := strings.TrimSpace(row.GroupBy)
		if key == "" {
			continue
		}
		if row.TimeBucket >= latestTs[key] {
			latestTs[key] = row.TimeBucket
			aggregated[key] = DatastoreErrorRow{
				ErrorType:    key,
				ErrorsPerSec: derefFloat(row.ErrorsPerSec),
			}
		}
	}

	result := mapsToSortedRows(aggregated, func(left, right DatastoreErrorRow) bool {
		if left.ErrorsPerSec == right.ErrorsPerSec {
			return left.ErrorType < right.ErrorType
		}
		return left.ErrorsPerSec > right.ErrorsPerSec
	})
	return result, nil
}

func (s *Service) GetDatastoreSystemConnections(ctx context.Context, teamID int64, startMs, endMs int64, system string) ([]DatastoreConnectionRow, error) {
	filter := dbfilter.Filters{DBSystem: []string{system}}

	var (
		counts      []dbconnections.ConnectionCountPoint
		utilRows    []dbconnections.ConnectionUtilPoint
		pendingRows []dbconnections.PendingRequestsPoint
		timeoutRows []dbconnections.ConnectionTimeoutPoint
		waitRows    []dbconnections.PoolLatencyPoint
		limitRows   []dbconnections.ConnectionLimits
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		counts, err = s.dbConnections.GetConnectionCountSeries(gctx, teamID, startMs, endMs, filter)
		return err
	})
	g.Go(func() error {
		var err error
		utilRows, err = s.dbConnections.GetConnectionUtilization(gctx, teamID, startMs, endMs, filter)
		return err
	})
	g.Go(func() error {
		var err error
		pendingRows, err = s.dbConnections.GetPendingRequests(gctx, teamID, startMs, endMs, filter)
		return err
	})
	g.Go(func() error {
		var err error
		timeoutRows, err = s.dbConnections.GetConnectionTimeoutRate(gctx, teamID, startMs, endMs, filter)
		return err
	})
	g.Go(func() error {
		var err error
		waitRows, err = s.dbConnections.GetConnectionWaitTime(gctx, teamID, startMs, endMs, filter)
		return err
	})
	g.Go(func() error {
		var err error
		limitRows, err = s.dbConnections.GetConnectionLimits(gctx, teamID, startMs, endMs, filter)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	aggregated := map[string]DatastoreConnectionRow{}
	countTs := map[string]string{}
	utilTs := map[string]string{}
	pendingTs := map[string]string{}
	timeoutTs := map[string]string{}
	waitTs := map[string]string{}

	for _, row := range counts {
		if row.State != "used" {
			continue
		}
		key := strings.TrimSpace(row.PoolName)
		if key == "" {
			continue
		}
		if row.TimeBucket >= countTs[key] {
			countTs[key] = row.TimeBucket
			current := aggregated[key]
			current.PoolName = key
			current.UsedConnections = derefFloat(row.Count)
			aggregated[key] = current
		}
	}

	for _, row := range utilRows {
		key := strings.TrimSpace(row.PoolName)
		if key == "" {
			continue
		}
		if row.TimeBucket >= utilTs[key] {
			utilTs[key] = row.TimeBucket
			current := aggregated[key]
			current.PoolName = key
			current.UtilPct = derefFloat(row.UtilPct)
			aggregated[key] = current
		}
	}

	for _, row := range pendingRows {
		key := strings.TrimSpace(row.PoolName)
		if key == "" {
			continue
		}
		if row.TimeBucket >= pendingTs[key] {
			pendingTs[key] = row.TimeBucket
			current := aggregated[key]
			current.PoolName = key
			current.PendingRequests = derefFloat(row.Count)
			aggregated[key] = current
		}
	}

	for _, row := range timeoutRows {
		key := strings.TrimSpace(row.PoolName)
		if key == "" {
			continue
		}
		if row.TimeBucket >= timeoutTs[key] {
			timeoutTs[key] = row.TimeBucket
			current := aggregated[key]
			current.PoolName = key
			current.TimeoutRate = derefFloat(row.TimeoutRate)
			aggregated[key] = current
		}
	}

	for _, row := range waitRows {
		key := strings.TrimSpace(row.PoolName)
		if key == "" {
			continue
		}
		if row.TimeBucket >= waitTs[key] {
			waitTs[key] = row.TimeBucket
			current := aggregated[key]
			current.PoolName = key
			current.P95WaitMs = derefFloat(row.P95Ms)
			aggregated[key] = current
		}
	}

	for _, row := range limitRows {
		key := strings.TrimSpace(row.PoolName)
		if key == "" {
			continue
		}
		current := aggregated[key]
		current.PoolName = key
		current.MaxConnections = derefFloat(row.Max)
		current.IdleMax = derefFloat(row.IdleMax)
		current.IdleMin = derefFloat(row.IdleMin)
		aggregated[key] = current
	}

	result := mapsToSortedRows(aggregated, func(left, right DatastoreConnectionRow) bool {
		if left.UtilPct == right.UtilPct {
			return left.PoolName < right.PoolName
		}
		return left.UtilPct > right.UtilPct
	})
	return result, nil
}

func (s *Service) GetDatastoreSystemSlowQueries(ctx context.Context, teamID int64, startMs, endMs int64, system string) ([]dbslowqueries.SlowQueryPattern, error) {
	return s.dbSlowQueries.GetSlowQueryPatterns(ctx, teamID, startMs, endMs, dbfilter.Filters{
		DBSystem: []string{system},
	}, 25)
}


func isRedisSystem(system string) bool {
	return strings.EqualFold(strings.TrimSpace(system), "redis")
}

func datastoreCategory(system string) string {
	if isRedisSystem(system) {
		return "redis"
	}
	return "database"
}

func derefFloat(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}

func safeRatioPct(numerator int64, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator) * 100
}

func latestReadOps(rows []dbvolume.ReadWritePoint) float64 {
	latestTs := ""
	value := 0.0
	for _, row := range rows {
		if row.TimeBucket >= latestTs {
			latestTs = row.TimeBucket
			value = derefFloat(row.ReadOpsPerSec)
		}
	}
	return value
}

func latestWriteOps(rows []dbvolume.ReadWritePoint) float64 {
	latestTs := ""
	value := 0.0
	for _, row := range rows {
		if row.TimeBucket >= latestTs {
			latestTs = row.TimeBucket
			value = derefFloat(row.WriteOpsPerSec)
		}
	}
	return value
}

func errorCountFromDetected(row *dbsystems.DetectedSystem) int64 {
	if row == nil {
		return 0
	}
	return row.ErrorCount
}

func serverHintFromDetected(row *dbsystems.DetectedSystem) string {
	if row == nil {
		return ""
	}
	return row.ServerAddress
}

func mapsToSortedRows[T any](input map[string]T, less func(left, right T) bool) []T {
	rows := make([]T, 0, len(input))
	for _, row := range input {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		return less(rows[i], rows[j])
	})
	return rows
}

func trendRows[T any](input map[string]T, less func(left, right T) bool) []T {
	rows := make([]T, 0, len(input))
	for _, row := range input {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		return less(rows[i], rows[j])
	})
	return rows
}

