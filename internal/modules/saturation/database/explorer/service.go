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
		summary, err = s.dbSummary.GetSummaryStats(gctx, teamID, startMs, endMs, dbshared.Filters{})
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
		summary, err = s.dbSummary.GetSummaryStats(gctx, teamID, startMs, endMs, dbshared.Filters{
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
		readWrite, err = s.dbVolume.GetReadVsWrite(gctx, teamID, startMs, endMs, dbshared.Filters{
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
	var (
		latencyRows []dbsystem.LatencyTimeSeries
		opsRows     []dbsystem.OpsTimeSeries
		errorRows   []dbsystem.ErrorTimeSeries
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		latencyRows, err = s.dbSystem.GetSystemLatency(gctx, teamID, startMs, endMs, system, dbshared.Filters{})
		return err
	})
	g.Go(func() error {
		var err error
		opsRows, err = s.dbSystem.GetSystemOps(gctx, teamID, startMs, endMs, system, dbshared.Filters{})
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
	return s.dbSlowQueries.GetSlowQueryPatterns(ctx, teamID, startMs, endMs, dbshared.Filters{
		DBSystem: []string{system},
	}, 25)
}

const (
	kafkaMetricBytesConsumedRate      = "kafka.consumer.bytes_consumed_rate"
	kafkaMetricBytesConsumedTotal     = "kafka.consumer.bytes_consumed_total"
	kafkaMetricRecordsConsumedRate    = "kafka.consumer.records_consumed_rate"
	kafkaMetricRecordsConsumedTotal   = "kafka.consumer.records_consumed_total"
	kafkaMetricRecordsLag             = "kafka.consumer.records_lag"
	kafkaMetricRecordsLead            = "kafka.consumer.records_lead"
	kafkaMetricAssignedPartitions     = "kafka.consumer.assigned_partitions"
	kafkaMetricCommitRate             = "kafka.consumer.commit_rate"
	kafkaMetricCommitLatencyAvg       = "kafka.consumer.commit_latency_avg"
	kafkaMetricCommitLatencyMax       = "kafka.consumer.commit_latency_max"
	kafkaMetricFetchRate              = "kafka.consumer.fetch_rate"
	kafkaMetricFetchLatencyAvg        = "kafka.consumer.fetch_latency_avg"
	kafkaMetricFetchLatencyMax        = "kafka.consumer.fetch_latency_max"
	kafkaMetricHeartbeatRate          = "kafka.consumer.heartbeat_rate"
	kafkaMetricFailedRebalancePerHour = "kafka.consumer.failed_rebalance_rate_per_hour"
	kafkaMetricPollIdleRatioAvg       = "kafka.consumer.poll_idle_ratio_avg"
	kafkaMetricLastPollSecondsAgo     = "kafka.consumer.last_poll_seconds_ago"
	kafkaMetricConnectionCount        = "kafka.consumer.connection_count"
)

var kafkaTopicMetricNames = []string{
	kafkaMetricBytesConsumedRate,
	kafkaMetricBytesConsumedTotal,
	kafkaMetricRecordsConsumedRate,
	kafkaMetricRecordsConsumedTotal,
	kafkaMetricRecordsLag,
	kafkaMetricRecordsLead,
}

var kafkaGroupMetricNames = []string{
	kafkaMetricAssignedPartitions,
	kafkaMetricCommitRate,
	kafkaMetricCommitLatencyAvg,
	kafkaMetricCommitLatencyMax,
	kafkaMetricFetchRate,
	kafkaMetricFetchLatencyAvg,
	kafkaMetricFetchLatencyMax,
	kafkaMetricHeartbeatRate,
	kafkaMetricFailedRebalancePerHour,
	kafkaMetricPollIdleRatioAvg,
	kafkaMetricLastPollSecondsAgo,
	kafkaMetricConnectionCount,
}

func (s *Service) GetKafkaSummary(ctx context.Context, teamID int64, startMs, endMs int64) (KafkaSummaryResponse, error) {
	var topics []KafkaTopicRow
	var groups []KafkaGroupRow

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		topics, err = s.buildKafkaTopicRows(gctx, teamID, startMs, endMs, saturationkafka.KafkaFilters{})
		return err
	})

	g.Go(func() error {
		var err error
		groups, err = s.buildKafkaGroupRows(gctx, teamID, startMs, endMs, saturationkafka.KafkaFilters{})
		return err
	})

	if err := g.Wait(); err != nil {
		return KafkaSummaryResponse{}, err
	}

	summary := KafkaSummaryResponse{
		TopicCount: len(topics),
		GroupCount: len(groups),
	}
	for _, row := range topics {
		summary.BytesPerSec += row.BytesPerSec
	}
	for _, row := range groups {
		summary.AssignedPartitions += row.AssignedPartitions
	}
	return summary, nil
}

func (s *Service) GetKafkaTopics(ctx context.Context, teamID int64, startMs, endMs int64) ([]KafkaTopicRow, error) {
	return s.buildKafkaTopicRows(ctx, teamID, startMs, endMs, saturationkafka.KafkaFilters{})
}

func (s *Service) GetKafkaGroups(ctx context.Context, teamID int64, startMs, endMs int64) ([]KafkaGroupRow, error) {
	return s.buildKafkaGroupRows(ctx, teamID, startMs, endMs, saturationkafka.KafkaFilters{})
}

func (s *Service) GetKafkaTopicOverview(ctx context.Context, teamID int64, startMs, endMs int64, topic string) (KafkaTopicOverview, error) {
	rows, err := s.buildKafkaTopicRows(ctx, teamID, startMs, endMs, saturationkafka.KafkaFilters{Topic: topic})
	if err != nil {
		return KafkaTopicOverview{}, err
	}
	overview := KafkaTopicOverview{
		Topic: topic,
		Trend: []KafkaTopicTrendPoint{},
	}
	if len(rows) > 0 {
		overview.Summary = rows[0]
	}
	return overview, nil
}

func (s *Service) GetKafkaTopicGroups(ctx context.Context, teamID int64, startMs, endMs int64, topic string) ([]KafkaTopicConsumerRow, error) {
	return s.buildKafkaTopicConsumerRows(ctx, teamID, startMs, endMs, saturationkafka.KafkaFilters{Topic: topic})
}

func (s *Service) GetKafkaTopicPartitions(ctx context.Context, teamID int64, startMs, endMs int64, topic string) ([]saturationkafka.PartitionLag, error) {
	return []saturationkafka.PartitionLag{}, nil
}

func (s *Service) GetKafkaGroupOverview(ctx context.Context, teamID int64, startMs, endMs int64, group string) (KafkaGroupOverview, error) {
	rows, err := s.buildKafkaGroupRows(ctx, teamID, startMs, endMs, saturationkafka.KafkaFilters{Group: group})
	if err != nil {
		return KafkaGroupOverview{}, err
	}
	overview := KafkaGroupOverview{
		ConsumerGroup: group,
		Trend:         []KafkaGroupTrendPoint{},
	}
	if len(rows) > 0 {
		overview.Summary = rows[0]
	}
	return overview, nil
}

func (s *Service) GetKafkaGroupTopics(ctx context.Context, teamID int64, startMs, endMs int64, group string) ([]KafkaTopicRow, error) {
	return s.buildKafkaTopicRows(ctx, teamID, startMs, endMs, saturationkafka.KafkaFilters{Group: group})
}

func (s *Service) GetKafkaGroupPartitions(ctx context.Context, teamID int64, startMs, endMs int64, group string) ([]saturationkafka.PartitionLag, error) {
	return []saturationkafka.PartitionLag{}, nil
}

func (s *Service) buildKafkaTopicRows(ctx context.Context, teamID int64, startMs, endMs int64, filters saturationkafka.KafkaFilters) ([]KafkaTopicRow, error) {
	samples, err := s.kafka.GetTopicMetricSamples(ctx, teamID, startMs, endMs, filters, kafkaTopicMetricNames)
	if err != nil {
		return nil, err
	}

	latestByKey := map[string]sampleValue{}
	consumerGroupsByTopic := map[string]map[string]struct{}{}
	for _, sample := range samples {
		topic := strings.TrimSpace(sample.Topic)
		consumerGroup := strings.TrimSpace(sample.ConsumerGroup)
		if topic == "" {
			continue
		}
		if consumerGroup != "" {
			if consumerGroupsByTopic[topic] == nil {
				consumerGroupsByTopic[topic] = map[string]struct{}{}
			}
			consumerGroupsByTopic[topic][consumerGroup] = struct{}{}
		}
		key := topic + "\x00" + consumerGroup + "\x00" + sample.MetricName
		if current, ok := latestByKey[key]; !ok || sample.Timestamp >= current.Timestamp {
			latestByKey[key] = sampleValue{Timestamp: sample.Timestamp, Value: sample.Value}
		}
	}

	rowsByTopic := map[string]*KafkaTopicRow{}
	for key, value := range latestByKey {
		topic, _, metricName := splitKafkaKey(key)
		row := ensureKafkaTopicRow(rowsByTopic, topic)
		switch metricName {
		case kafkaMetricBytesConsumedRate:
			row.BytesPerSec += value.Value
		case kafkaMetricBytesConsumedTotal:
			row.BytesTotal += value.Value
		case kafkaMetricRecordsConsumedRate:
			row.RecordsPerSec += value.Value
		case kafkaMetricRecordsConsumedTotal:
			row.RecordsTotal += value.Value
		case kafkaMetricRecordsLag:
			if value.Value > row.Lag {
				row.Lag = value.Value
			}
		case kafkaMetricRecordsLead:
			if value.Value > row.Lead {
				row.Lead = value.Value
			}
		}
	}

	rows := make([]KafkaTopicRow, 0, len(rowsByTopic))
	for topic, row := range rowsByTopic {
		if groups := consumerGroupsByTopic[topic]; groups != nil {
			row.ConsumerGroupCount = len(groups)
		}
		rows = append(rows, *row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Lag == rows[j].Lag {
			if rows[i].BytesPerSec == rows[j].BytesPerSec {
				return rows[i].Topic < rows[j].Topic
			}
			return rows[i].BytesPerSec > rows[j].BytesPerSec
		}
		return rows[i].Lag > rows[j].Lag
	})
	return rows, nil
}

func (s *Service) buildKafkaGroupRows(ctx context.Context, teamID int64, startMs, endMs int64, filters saturationkafka.KafkaFilters) ([]KafkaGroupRow, error) {
	var groupSamples []saturationkafka.ConsumerMetricSample
	var topicSamples []saturationkafka.TopicMetricSample

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		groupSamples, err = s.kafka.GetConsumerMetricSamples(gctx, teamID, startMs, endMs, filters, kafkaGroupMetricNames)
		return err
	})

	g.Go(func() error {
		var err error
		topicSamples, err = s.kafka.GetTopicMetricSamples(gctx, teamID, startMs, endMs, filters, kafkaTopicMetricNames)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	latestByKey := map[string]sampleValue{}
	for _, sample := range groupSamples {
		consumerGroup := strings.TrimSpace(sample.ConsumerGroup)
		if consumerGroup == "" {
			continue
		}
		key := consumerGroup + "\x00" + strings.TrimSpace(sample.NodeID) + "\x00" + sample.MetricName
		if current, ok := latestByKey[key]; !ok || sample.Timestamp >= current.Timestamp {
			latestByKey[key] = sampleValue{Timestamp: sample.Timestamp, Value: sample.Value}
		}
	}

	valuesByGroup := map[string]map[string][]float64{}
	for key, value := range latestByKey {
		consumerGroup, _, metricName := splitKafkaKey(key)
		if valuesByGroup[consumerGroup] == nil {
			valuesByGroup[consumerGroup] = map[string][]float64{}
		}
		valuesByGroup[consumerGroup][metricName] = append(valuesByGroup[consumerGroup][metricName], value.Value)
	}

	topicsByGroup := map[string]map[string]struct{}{}
	for _, sample := range topicSamples {
		consumerGroup := strings.TrimSpace(sample.ConsumerGroup)
		topic := strings.TrimSpace(sample.Topic)
		if consumerGroup == "" || topic == "" {
			continue
		}
		if topicsByGroup[consumerGroup] == nil {
			topicsByGroup[consumerGroup] = map[string]struct{}{}
		}
		topicsByGroup[consumerGroup][topic] = struct{}{}
	}

	groupSet := map[string]struct{}{}
	for group := range valuesByGroup {
		groupSet[group] = struct{}{}
	}
	for group := range topicsByGroup {
		groupSet[group] = struct{}{}
	}

	rows := make([]KafkaGroupRow, 0, len(groupSet))
	for _, consumerGroup := range sortedKeys(groupSet) {
		values := valuesByGroup[consumerGroup]
		row := KafkaGroupRow{
			ConsumerGroup:          consumerGroup,
			AssignedPartitions:     sumFloat(values[kafkaMetricAssignedPartitions]),
			CommitRate:             sumFloat(values[kafkaMetricCommitRate]),
			CommitLatencyAvgMs:     avgFloat(values[kafkaMetricCommitLatencyAvg]),
			CommitLatencyMaxMs:     maxFloat(values[kafkaMetricCommitLatencyMax]),
			FetchRate:              sumFloat(values[kafkaMetricFetchRate]),
			FetchLatencyAvgMs:      avgFloat(values[kafkaMetricFetchLatencyAvg]),
			FetchLatencyMaxMs:      maxFloat(values[kafkaMetricFetchLatencyMax]),
			HeartbeatRate:          sumFloat(values[kafkaMetricHeartbeatRate]),
			FailedRebalancePerHour: sumFloat(values[kafkaMetricFailedRebalancePerHour]),
			PollIdleRatio:          avgFloat(values[kafkaMetricPollIdleRatioAvg]),
			LastPollSecondsAgo:     maxFloat(values[kafkaMetricLastPollSecondsAgo]),
			ConnectionCount:        sumFloat(values[kafkaMetricConnectionCount]),
			TopicCount:             len(topicsByGroup[consumerGroup]),
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].AssignedPartitions == rows[j].AssignedPartitions {
			return rows[i].ConsumerGroup < rows[j].ConsumerGroup
		}
		return rows[i].AssignedPartitions > rows[j].AssignedPartitions
	})
	return rows, nil
}

func (s *Service) buildKafkaTopicConsumerRows(ctx context.Context, teamID int64, startMs, endMs int64, filters saturationkafka.KafkaFilters) ([]KafkaTopicConsumerRow, error) {
	samples, err := s.kafka.GetTopicMetricSamples(ctx, teamID, startMs, endMs, filters, kafkaTopicMetricNames)
	if err != nil {
		return nil, err
	}

	latestByKey := map[string]sampleValue{}
	for _, sample := range samples {
		consumerGroup := strings.TrimSpace(sample.ConsumerGroup)
		if consumerGroup == "" {
			continue
		}
		key := consumerGroup + "\x00" + strings.TrimSpace(sample.Topic) + "\x00" + sample.MetricName
		if current, ok := latestByKey[key]; !ok || sample.Timestamp >= current.Timestamp {
			latestByKey[key] = sampleValue{Timestamp: sample.Timestamp, Value: sample.Value}
		}
	}

	rowsByGroup := map[string]*KafkaTopicConsumerRow{}
	for key, value := range latestByKey {
		consumerGroup, _, metricName := splitKafkaKey(key)
		row := rowsByGroup[consumerGroup]
		if row == nil {
			row = &KafkaTopicConsumerRow{ConsumerGroup: consumerGroup}
			rowsByGroup[consumerGroup] = row
		}
		switch metricName {
		case kafkaMetricBytesConsumedRate:
			row.BytesPerSec += value.Value
		case kafkaMetricRecordsConsumedRate:
			row.RecordsPerSec += value.Value
		case kafkaMetricRecordsLag:
			if value.Value > row.Lag {
				row.Lag = value.Value
			}
		case kafkaMetricRecordsLead:
			if value.Value > row.Lead {
				row.Lead = value.Value
			}
		}
	}

	rows := make([]KafkaTopicConsumerRow, 0, len(rowsByGroup))
	for _, row := range rowsByGroup {
		rows = append(rows, *row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Lag == rows[j].Lag {
			return rows[i].ConsumerGroup < rows[j].ConsumerGroup
		}
		return rows[i].Lag > rows[j].Lag
	})
	return rows, nil
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

type sampleValue struct {
	Timestamp string
	Value     float64
}

func ensureKafkaTopicRow(rows map[string]*KafkaTopicRow, topic string) *KafkaTopicRow {
	row := rows[topic]
	if row == nil {
		row = &KafkaTopicRow{Topic: topic}
		rows[topic] = row
	}
	return row
}

func splitKafkaKey(key string) (string, string, string) {
	parts := strings.SplitN(key, "\x00", 3)
	for len(parts) < 3 {
		parts = append(parts, "")
	}
	return parts[0], parts[1], parts[2]
}

func sumFloat(values []float64) float64 {
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total
}

func avgFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return sumFloat(values) / float64(len(values))
}

func maxFloat(values []float64) float64 {
	max := 0.0
	for i, value := range values {
		if i == 0 || value > max {
			max = value
		}
	}
	return max
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
