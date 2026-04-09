package explorer

import (
	"context"
	"sort"
	"strings"

	dbconnections "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/connections"
	dberrors "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/errors"
	dbshared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
	dblatency "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/latency"
	dbslowqueries "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/slowqueries"
	dbsummary "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/summary"
	dbsystem "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/system"
	dbsystems "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/systems"
	dbvolume "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/volume"
	saturationkafka "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka"
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
	kafka         *saturationkafka.KafkaService
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
	kafkaSvc *saturationkafka.KafkaService,
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
		kafka:         kafkaSvc,
	}
}

func (s *Service) GetDatastoreSummary(ctx context.Context, teamID int64, startMs, endMs int64) (DatastoreSummaryResponse, error) {
	systemRows, err := s.dbSystems.GetDetectedSystems(ctx, teamID, startMs, endMs)
	if err != nil {
		return DatastoreSummaryResponse{}, err
	}

	summary, err := s.dbSummary.GetSummaryStats(ctx, teamID, startMs, endMs, dbshared.Filters{})
	if err != nil {
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
	systemRows, err := s.dbSystems.GetDetectedSystems(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	rows := make([]DatastoreSystemRow, 0, len(systemRows))
	for _, detected := range systemRows {
		summary, err := s.dbSummary.GetSummaryStats(ctx, teamID, startMs, endMs, dbshared.Filters{
			DBSystem: []string{detected.DBSystem},
		})
		if err != nil {
			return nil, err
		}

		rows = append(rows, DatastoreSystemRow{
			System:            detected.DBSystem,
			Category:          datastoreCategory(detected.DBSystem),
			QueryCount:        detected.QueryCount,
			AvgLatencyMs:      detected.AvgLatencyMs,
			P95LatencyMs:      derefFloat(summary.P95LatencyMs),
			ErrorRate:         safeRatioPct(detected.ErrorCount, detected.QueryCount),
			ActiveConnections: summary.ActiveConnections,
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
	summary, err := s.dbSummary.GetSummaryStats(ctx, teamID, startMs, endMs, dbshared.Filters{
		DBSystem: []string{system},
	})
	if err != nil {
		return DatastoreSystemOverview{}, err
	}

	systems, err := s.dbSystems.GetDetectedSystems(ctx, teamID, startMs, endMs)
	if err != nil {
		return DatastoreSystemOverview{}, err
	}

	namespaces, err := s.dbSystem.GetSystemNamespaces(ctx, teamID, startMs, endMs, system)
	if err != nil {
		return DatastoreSystemOverview{}, err
	}

	topCollections, err := s.dbSystem.GetSystemTopCollectionsByLatency(ctx, teamID, startMs, endMs, system)
	if err != nil {
		return DatastoreSystemOverview{}, err
	}

	readWrite, err := s.dbVolume.GetReadVsWrite(ctx, teamID, startMs, endMs, dbshared.Filters{
		DBSystem: []string{system},
	})
	if err != nil {
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
	rows, err := s.dbLatency.GetLatencyByServer(ctx, teamID, startMs, endMs, dbshared.Filters{
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
	latencyRows, err := s.dbSystem.GetSystemLatency(ctx, teamID, startMs, endMs, system, dbshared.Filters{})
	if err != nil {
		return nil, err
	}
	opsRows, err := s.dbSystem.GetSystemOps(ctx, teamID, startMs, endMs, system, dbshared.Filters{})
	if err != nil {
		return nil, err
	}
	errorRows, err := s.dbSystem.GetSystemErrors(ctx, teamID, startMs, endMs, system)
	if err != nil {
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
	rows, err := s.dbErrors.GetErrorsByErrorType(ctx, teamID, startMs, endMs, dbshared.Filters{
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
	filter := dbshared.Filters{DBSystem: []string{system}}

	counts, err := s.dbConnections.GetConnectionCountSeries(ctx, teamID, startMs, endMs, filter)
	if err != nil {
		return nil, err
	}
	utilRows, err := s.dbConnections.GetConnectionUtilization(ctx, teamID, startMs, endMs, filter)
	if err != nil {
		return nil, err
	}
	pendingRows, err := s.dbConnections.GetPendingRequests(ctx, teamID, startMs, endMs, filter)
	if err != nil {
		return nil, err
	}
	timeoutRows, err := s.dbConnections.GetConnectionTimeoutRate(ctx, teamID, startMs, endMs, filter)
	if err != nil {
		return nil, err
	}
	waitRows, err := s.dbConnections.GetConnectionWaitTime(ctx, teamID, startMs, endMs, filter)
	if err != nil {
		return nil, err
	}
	limitRows, err := s.dbConnections.GetConnectionLimits(ctx, teamID, startMs, endMs, filter)
	if err != nil {
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
	return s.dbSlowQueries.GetSlowQueryPatterns(ctx, teamID, startMs, endMs, dbshared.Filters{
		DBSystem: []string{system},
	}, 25)
}

func (s *Service) GetKafkaSummary(teamID int64, startMs, endMs int64) (KafkaSummaryResponse, error) {
	summary, err := s.kafka.GetKafkaSummaryStats(teamID, startMs, endMs, saturationkafka.KafkaFilters{})
	if err != nil {
		return KafkaSummaryResponse{}, err
	}

	topics, err := s.buildKafkaTopicRows(teamID, startMs, endMs, saturationkafka.KafkaFilters{})
	if err != nil {
		return KafkaSummaryResponse{}, err
	}
	groups, err := s.buildKafkaGroupRows(teamID, startMs, endMs, saturationkafka.KafkaFilters{})
	if err != nil {
		return KafkaSummaryResponse{}, err
	}

	return KafkaSummaryResponse{
		PublishRatePerSec: summary.PublishRatePerSec,
		ReceiveRatePerSec: summary.ReceiveRatePerSec,
		MaxLag:            summary.MaxLag,
		PublishP95Ms:      summary.PublishP95Ms,
		ReceiveP95Ms:      summary.ReceiveP95Ms,
		TopicCount:        len(topics),
		GroupCount:        len(groups),
	}, nil
}

func (s *Service) GetKafkaTopics(teamID int64, startMs, endMs int64) ([]KafkaTopicRow, error) {
	return s.buildKafkaTopicRows(teamID, startMs, endMs, saturationkafka.KafkaFilters{})
}

func (s *Service) GetKafkaGroups(teamID int64, startMs, endMs int64) ([]KafkaGroupRow, error) {
	return s.buildKafkaGroupRows(teamID, startMs, endMs, saturationkafka.KafkaFilters{})
}

func (s *Service) GetKafkaTopicOverview(teamID int64, startMs, endMs int64, topic string) (KafkaTopicOverview, error) {
	rows, err := s.buildKafkaTopicRows(teamID, startMs, endMs, saturationkafka.KafkaFilters{Topic: topic})
	if err != nil {
		return KafkaTopicOverview{}, err
	}
	overview := KafkaTopicOverview{
		Topic: topic,
	}
	if len(rows) > 0 {
		overview.Summary = rows[0]
	}
	overview.Trend, err = s.buildKafkaTopicTrend(teamID, startMs, endMs, saturationkafka.KafkaFilters{Topic: topic})
	if err != nil {
		return KafkaTopicOverview{}, err
	}
	return overview, nil
}

func (s *Service) GetKafkaTopicGroups(teamID int64, startMs, endMs int64, topic string) ([]KafkaGroupRow, error) {
	return s.buildKafkaGroupRows(teamID, startMs, endMs, saturationkafka.KafkaFilters{Topic: topic})
}

func (s *Service) GetKafkaTopicPartitions(teamID int64, startMs, endMs int64, topic string) ([]saturationkafka.PartitionLag, error) {
	return s.kafka.GetConsumerLagPerPartition(teamID, startMs, endMs, saturationkafka.KafkaFilters{Topic: topic})
}

func (s *Service) GetKafkaGroupOverview(teamID int64, startMs, endMs int64, group string) (KafkaGroupOverview, error) {
	rows, err := s.buildKafkaGroupRows(teamID, startMs, endMs, saturationkafka.KafkaFilters{Group: group})
	if err != nil {
		return KafkaGroupOverview{}, err
	}
	overview := KafkaGroupOverview{
		ConsumerGroup: group,
	}
	if len(rows) > 0 {
		overview.Summary = rows[0]
	}
	overview.Trend, err = s.buildKafkaGroupTrend(teamID, startMs, endMs, saturationkafka.KafkaFilters{Group: group})
	if err != nil {
		return KafkaGroupOverview{}, err
	}
	return overview, nil
}

func (s *Service) GetKafkaGroupTopics(teamID int64, startMs, endMs int64, group string) ([]KafkaTopicRow, error) {
	return s.buildKafkaTopicRows(teamID, startMs, endMs, saturationkafka.KafkaFilters{Group: group})
}

func (s *Service) GetKafkaGroupPartitions(teamID int64, startMs, endMs int64, group string) ([]saturationkafka.PartitionLag, error) {
	return s.kafka.GetConsumerLagPerPartition(teamID, startMs, endMs, saturationkafka.KafkaFilters{Group: group})
}

func (s *Service) buildKafkaTopicRows(teamID int64, startMs, endMs int64, filters saturationkafka.KafkaFilters) ([]KafkaTopicRow, error) {
	produceRates, err := s.kafka.GetProduceRateByTopic(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	consumeRates, err := s.kafka.GetConsumeRateByTopic(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	publishLatency, err := s.kafka.GetPublishLatencyByTopic(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	receiveLatency, err := s.kafka.GetReceiveLatencyByTopic(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	e2eLatency, err := s.kafka.GetE2ELatency(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	lagRows, err := s.kafka.GetConsumerLagByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	publishErrors, err := s.kafka.GetPublishErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	consumeErrors, err := s.kafka.GetConsumeErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	processErrors, err := s.kafka.GetProcessErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	clientErrors, err := s.kafka.GetClientOpErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}

	latestProduce := latestTopicRates(produceRates)
	latestConsume := latestTopicRates(consumeRates)
	latestPublish := latestTopicLatencies(publishLatency)
	latestReceive := latestTopicLatencies(receiveLatency)
	latestE2E := latestE2ELatencies(e2eLatency)
	maxLag := maxLagByTopic(lagRows)
	groupCounts := distinctGroupCountsByTopic(lagRows)
	errorRates := mergeMaxFloatMaps(
		maxTopicErrors(publishErrors),
		maxTopicErrors(consumeErrors),
		maxTopicErrors(processErrors),
		maxTopicErrors(clientErrors),
	)

	keys := unionKeysFromStringMaps(
		latestProduce,
		latestConsume,
		maxLag,
		errorRates,
	)
	keys = unionKeysWithTopicLatencies(keys, latestPublish, latestReceive)
	keys = unionKeysWithE2ELatencies(keys, latestE2E)
	keys = unionKeysWithCounts(keys, groupCounts)

	rows := make([]KafkaTopicRow, 0, len(keys))
	for _, topic := range keys {
		publish := latestPublish[topic]
		receive := latestReceive[topic]
		e2e := latestE2E[topic]
		rows = append(rows, KafkaTopicRow{
			Topic:              topic,
			ProduceRatePerSec:  latestProduce[topic],
			ConsumeRatePerSec:  latestConsume[topic],
			MaxLag:             maxLag[topic],
			E2EP95Ms:           e2e.ProcessP95Ms,
			PublishP95Ms:       publish.P95Ms,
			ReceiveP95Ms:       receive.P95Ms,
			ErrorRate:          errorRates[topic],
			ConsumerGroupCount: groupCounts[topic],
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].MaxLag == rows[j].MaxLag {
			return rows[i].Topic < rows[j].Topic
		}
		return rows[i].MaxLag > rows[j].MaxLag
	})
	return rows, nil
}

func (s *Service) buildKafkaGroupRows(teamID int64, startMs, endMs int64, filters saturationkafka.KafkaFilters) ([]KafkaGroupRow, error) {
	consumeRates, err := s.kafka.GetConsumeRateByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	processRates, err := s.kafka.GetProcessRateByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	processLatency, err := s.kafka.GetProcessLatencyByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	lagRows, err := s.kafka.GetConsumerLagByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	rebalanceRows, err := s.kafka.GetRebalanceSignals(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	consumeErrors, err := s.kafka.GetConsumeErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	processErrors, err := s.kafka.GetProcessErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	clientErrors, err := s.kafka.GetClientOpErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}

	latestConsume := latestGroupRates(consumeRates)
	latestProcess := latestGroupRates(processRates)
	latestLatency := latestGroupLatencies(processLatency)
	maxLag := maxLagByGroup(lagRows)
	topicCounts := distinctTopicCountsByGroup(lagRows)
	latestRebalance := latestRebalanceByGroup(rebalanceRows)
	errorRates := mergeMaxFloatMaps(
		maxGroupErrors(consumeErrors),
		maxGroupErrors(processErrors),
		maxGroupErrors(clientErrors),
	)

	keys := unionKeysFromStringMaps(latestConsume, latestProcess, maxLag, errorRates)
	keys = unionKeysWithGroupLatency(keys, latestLatency, latestRebalance)
	keys = unionKeysWithCounts(keys, topicCounts)

	rows := make([]KafkaGroupRow, 0, len(keys))
	for _, group := range keys {
		latency := latestLatency[group]
		rebalance := latestRebalance[group]
		rows = append(rows, KafkaGroupRow{
			ConsumerGroup:      group,
			Lag:                maxLag[group],
			ConsumeRatePerSec:  latestConsume[group],
			ProcessRatePerSec:  latestProcess[group],
			ProcessP95Ms:       latency.P95Ms,
			ErrorRate:          errorRates[group],
			RebalanceRate:      rebalance.RebalanceRate,
			AssignedPartitions: rebalance.AssignedPartitions,
			TopicCount:         topicCounts[group],
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Lag == rows[j].Lag {
			return rows[i].ConsumerGroup < rows[j].ConsumerGroup
		}
		return rows[i].Lag > rows[j].Lag
	})
	return rows, nil
}

func (s *Service) buildKafkaTopicTrend(teamID int64, startMs, endMs int64, filters saturationkafka.KafkaFilters) ([]KafkaTopicTrendPoint, error) {
	produceRates, err := s.kafka.GetProduceRateByTopic(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	consumeRates, err := s.kafka.GetConsumeRateByTopic(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	publishLatency, err := s.kafka.GetPublishLatencyByTopic(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	receiveLatency, err := s.kafka.GetReceiveLatencyByTopic(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	e2eLatency, err := s.kafka.GetE2ELatency(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	lagRows, err := s.kafka.GetConsumerLagByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	publishErrors, err := s.kafka.GetPublishErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	consumeErrors, err := s.kafka.GetConsumeErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	processErrors, err := s.kafka.GetProcessErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	clientErrors, err := s.kafka.GetClientOpErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}

	aggregated := map[string]KafkaTopicTrendPoint{}
	for _, row := range produceRates {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		acc.ProduceRatePerSec = row.RatePerSec
		aggregated[row.Timestamp] = acc
	}
	for _, row := range consumeRates {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		acc.ConsumeRatePerSec = row.RatePerSec
		aggregated[row.Timestamp] = acc
	}
	for _, row := range publishLatency {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		acc.PublishP95Ms = row.P95Ms
		aggregated[row.Timestamp] = acc
	}
	for _, row := range receiveLatency {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		acc.ReceiveP95Ms = row.P95Ms
		aggregated[row.Timestamp] = acc
	}
	for _, row := range e2eLatency {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		acc.E2EP95Ms = row.ProcessP95Ms
		aggregated[row.Timestamp] = acc
	}
	for _, row := range lagRows {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		if row.Lag > acc.MaxLag {
			acc.MaxLag = row.Lag
		}
		aggregated[row.Timestamp] = acc
	}
	for _, set := range [][]saturationkafka.ErrorRatePoint{publishErrors, consumeErrors, processErrors, clientErrors} {
		for _, row := range set {
			acc := aggregated[row.Timestamp]
			acc.Timestamp = row.Timestamp
			if row.ErrorRate > acc.ErrorRate {
				acc.ErrorRate = row.ErrorRate
			}
			aggregated[row.Timestamp] = acc
		}
	}

	return trendRows(aggregated, func(left, right KafkaTopicTrendPoint) bool {
		return left.Timestamp < right.Timestamp
	}), nil
}

func (s *Service) buildKafkaGroupTrend(teamID int64, startMs, endMs int64, filters saturationkafka.KafkaFilters) ([]KafkaGroupTrendPoint, error) {
	consumeRates, err := s.kafka.GetConsumeRateByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	processRates, err := s.kafka.GetProcessRateByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	processLatency, err := s.kafka.GetProcessLatencyByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	lagRows, err := s.kafka.GetConsumerLagByGroup(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	rebalanceRows, err := s.kafka.GetRebalanceSignals(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	consumeErrors, err := s.kafka.GetConsumeErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	processErrors, err := s.kafka.GetProcessErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}
	clientErrors, err := s.kafka.GetClientOpErrors(teamID, startMs, endMs, filters)
	if err != nil {
		return nil, err
	}

	aggregated := map[string]KafkaGroupTrendPoint{}
	for _, row := range consumeRates {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		acc.ConsumeRatePerSec = row.RatePerSec
		aggregated[row.Timestamp] = acc
	}
	for _, row := range processRates {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		acc.ProcessRatePerSec = row.RatePerSec
		aggregated[row.Timestamp] = acc
	}
	for _, row := range processLatency {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		acc.ProcessP95Ms = row.P95Ms
		aggregated[row.Timestamp] = acc
	}
	for _, row := range lagRows {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		if row.Lag > acc.MaxLag {
			acc.MaxLag = row.Lag
		}
		aggregated[row.Timestamp] = acc
	}
	for _, row := range rebalanceRows {
		acc := aggregated[row.Timestamp]
		acc.Timestamp = row.Timestamp
		acc.RebalanceRate = row.RebalanceRate
		acc.AssignedPartitions = row.AssignedPartitions
		aggregated[row.Timestamp] = acc
	}
	for _, set := range [][]saturationkafka.ErrorRatePoint{consumeErrors, processErrors, clientErrors} {
		for _, row := range set {
			acc := aggregated[row.Timestamp]
			acc.Timestamp = row.Timestamp
			if row.ErrorRate > acc.ErrorRate {
				acc.ErrorRate = row.ErrorRate
			}
			aggregated[row.Timestamp] = acc
		}
	}

	return trendRows(aggregated, func(left, right KafkaGroupTrendPoint) bool {
		return left.Timestamp < right.Timestamp
	}), nil
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

func latestTopicRates(rows []saturationkafka.TopicRatePoint) map[string]float64 {
	values := map[string]float64{}
	latest := map[string]string{}
	for _, row := range rows {
		key := strings.TrimSpace(row.Topic)
		if key == "" {
			continue
		}
		if row.Timestamp >= latest[key] {
			latest[key] = row.Timestamp
			values[key] = row.RatePerSec
		}
	}
	return values
}

func latestTopicLatencies(rows []saturationkafka.TopicLatencyPoint) map[string]saturationkafka.TopicLatencyPoint {
	values := map[string]saturationkafka.TopicLatencyPoint{}
	latest := map[string]string{}
	for _, row := range rows {
		key := strings.TrimSpace(row.Topic)
		if key == "" {
			continue
		}
		if row.Timestamp >= latest[key] {
			latest[key] = row.Timestamp
			values[key] = row
		}
	}
	return values
}

func latestE2ELatencies(rows []saturationkafka.E2ELatencyPoint) map[string]saturationkafka.E2ELatencyPoint {
	values := map[string]saturationkafka.E2ELatencyPoint{}
	latest := map[string]string{}
	for _, row := range rows {
		key := strings.TrimSpace(row.Topic)
		if key == "" {
			continue
		}
		if row.Timestamp >= latest[key] {
			latest[key] = row.Timestamp
			values[key] = row
		}
	}
	return values
}

func latestGroupRates(rows []saturationkafka.GroupRatePoint) map[string]float64 {
	values := map[string]float64{}
	latest := map[string]string{}
	for _, row := range rows {
		key := strings.TrimSpace(row.ConsumerGroup)
		if key == "" {
			continue
		}
		if row.Timestamp >= latest[key] {
			latest[key] = row.Timestamp
			values[key] = row.RatePerSec
		}
	}
	return values
}

func latestGroupLatencies(rows []saturationkafka.GroupLatencyPoint) map[string]saturationkafka.GroupLatencyPoint {
	values := map[string]saturationkafka.GroupLatencyPoint{}
	latest := map[string]string{}
	for _, row := range rows {
		key := strings.TrimSpace(row.ConsumerGroup)
		if key == "" {
			continue
		}
		if row.Timestamp >= latest[key] {
			latest[key] = row.Timestamp
			values[key] = row
		}
	}
	return values
}

func latestRebalanceByGroup(rows []saturationkafka.RebalancePoint) map[string]saturationkafka.RebalancePoint {
	values := map[string]saturationkafka.RebalancePoint{}
	latest := map[string]string{}
	for _, row := range rows {
		key := strings.TrimSpace(row.ConsumerGroup)
		if key == "" {
			continue
		}
		if row.Timestamp >= latest[key] {
			latest[key] = row.Timestamp
			values[key] = row
		}
	}
	return values
}

func maxLagByTopic(rows []saturationkafka.LagPoint) map[string]float64 {
	values := map[string]float64{}
	for _, row := range rows {
		key := strings.TrimSpace(row.Topic)
		if key == "" {
			continue
		}
		if row.Lag > values[key] {
			values[key] = row.Lag
		}
	}
	return values
}

func maxLagByGroup(rows []saturationkafka.LagPoint) map[string]float64 {
	values := map[string]float64{}
	for _, row := range rows {
		key := strings.TrimSpace(row.ConsumerGroup)
		if key == "" {
			continue
		}
		if row.Lag > values[key] {
			values[key] = row.Lag
		}
	}
	return values
}

func distinctGroupCountsByTopic(rows []saturationkafka.LagPoint) map[string]int {
	sets := map[string]map[string]struct{}{}
	for _, row := range rows {
		topic := strings.TrimSpace(row.Topic)
		group := strings.TrimSpace(row.ConsumerGroup)
		if topic == "" || group == "" {
			continue
		}
		if sets[topic] == nil {
			sets[topic] = map[string]struct{}{}
		}
		sets[topic][group] = struct{}{}
	}
	values := map[string]int{}
	for key, set := range sets {
		values[key] = len(set)
	}
	return values
}

func distinctTopicCountsByGroup(rows []saturationkafka.LagPoint) map[string]int {
	sets := map[string]map[string]struct{}{}
	for _, row := range rows {
		topic := strings.TrimSpace(row.Topic)
		group := strings.TrimSpace(row.ConsumerGroup)
		if topic == "" || group == "" {
			continue
		}
		if sets[group] == nil {
			sets[group] = map[string]struct{}{}
		}
		sets[group][topic] = struct{}{}
	}
	values := map[string]int{}
	for key, set := range sets {
		values[key] = len(set)
	}
	return values
}

func maxTopicErrors(rows []saturationkafka.ErrorRatePoint) map[string]float64 {
	values := map[string]float64{}
	for _, row := range rows {
		key := strings.TrimSpace(row.Topic)
		if key == "" {
			continue
		}
		if row.ErrorRate > values[key] {
			values[key] = row.ErrorRate
		}
	}
	return values
}

func maxGroupErrors(rows []saturationkafka.ErrorRatePoint) map[string]float64 {
	values := map[string]float64{}
	for _, row := range rows {
		key := strings.TrimSpace(row.ConsumerGroup)
		if key == "" {
			continue
		}
		if row.ErrorRate > values[key] {
			values[key] = row.ErrorRate
		}
	}
	return values
}

func mergeMaxFloatMaps(maps ...map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for _, current := range maps {
		for key, value := range current {
			if value > out[key] {
				out[key] = value
			}
		}
	}
	return out
}

func unionKeysFromStringMaps(maps ...map[string]float64) []string {
	set := map[string]struct{}{}
	for _, current := range maps {
		for key := range current {
			if strings.TrimSpace(key) == "" {
				continue
			}
			set[key] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func unionKeysWithTopicLatencies(keys []string, topicLatencies ...map[string]saturationkafka.TopicLatencyPoint) []string {
	set := map[string]struct{}{}
	for _, key := range keys {
		set[key] = struct{}{}
	}
	for _, current := range topicLatencies {
		for key := range current {
			if strings.TrimSpace(key) == "" {
				continue
			}
			set[key] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func unionKeysWithE2ELatencies(keys []string, latencies map[string]saturationkafka.E2ELatencyPoint) []string {
	set := map[string]struct{}{}
	for _, key := range keys {
		set[key] = struct{}{}
	}
	for key := range latencies {
		if strings.TrimSpace(key) == "" {
			continue
		}
		set[key] = struct{}{}
	}
	return sortedKeys(set)
}

func unionKeysWithGroupLatency(keys []string, groupLatency map[string]saturationkafka.GroupLatencyPoint, rebalance map[string]saturationkafka.RebalancePoint) []string {
	set := map[string]struct{}{}
	for _, key := range keys {
		set[key] = struct{}{}
	}
	for key := range groupLatency {
		if strings.TrimSpace(key) == "" {
			continue
		}
		set[key] = struct{}{}
	}
	for key := range rebalance {
		if strings.TrimSpace(key) == "" {
			continue
		}
		set[key] = struct{}{}
	}
	return sortedKeys(set)
}

func unionKeysWithCounts(keys []string, counts map[string]int) []string {
	set := map[string]struct{}{}
	for _, key := range keys {
		set[key] = struct{}{}
	}
	for key := range counts {
		if strings.TrimSpace(key) == "" {
			continue
		}
		set[key] = struct{}{}
	}
	return sortedKeys(set)
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
