package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	chembed "github.com/Optikk-Org/optikk-backend/db/clickhouse"
	"github.com/Optikk-Org/optikk-backend/internal/auth"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database/chmigrate"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/redis"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	ingestlogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/logs"
	ingestmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics"
	ingestspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/spans"
	indexer "github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/indexer"
	"github.com/twmb/franz-go/pkg/kgo"
	redigoredis "github.com/gomodule/redigo/redis"
	goredis "github.com/redis/go-redis/v9"
)

// IngestModules bundles the three ingestion modules returned to the registry.
// Held as a struct (instead of a slice) so modules_manifest.go can spread them
// in a readable order alongside non-ingest modules.
type IngestModules struct {
	Logs    *ingestlogs.Module
	Metrics *ingestmetrics.Module
	Spans   *ingestspans.Module
}

// Infra holds process-wide infrastructure constructed at startup.
type Infra struct {
	DB             *sql.DB
	CH             clickhouse.Conn
	SessionManager session.Manager
	RedisClient    *goredis.Client
	RedisPool      *redigoredis.Pool
	Authenticator  *auth.Authenticator
	Ingest         IngestModules

	producerClient    *kgo.Client
	consumerClients   []*kgo.Client // closed on shutdown alongside producer
}

func newInfra(cfg config.Config) (_ *Infra, err error) {
	dbConn, err := dbutil.Open(cfg.MySQLDSN(), cfg.MySQL.MaxOpenConns, cfg.MySQL.MaxIdleConns)
	if err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}
	defer func() {
		if err != nil {
			_ = dbConn.Close() //nolint:errcheck // cleanup after failed init
		}
	}()

	chConn, err := openClickHouse(cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = chConn.Close() //nolint:errcheck // cleanup after failed init
		}
	}()

	redisClients, err := redis.NewClients(cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			redisClients.Pool.Close()
			_ = redisClients.Client.Close() //nolint:errcheck
		}
	}()

	sessionManager, err := newSessionManager(cfg, redisClients.Pool)
	if err != nil {
		return nil, err
	}

	prefix := cfg.KafkaTopicPrefix()
	kafkaCfg := kafkainfra.Config{Brokers: cfg.KafkaBrokers()}
	rf := int16(cfg.KafkaReplicationFactor()) //nolint:gosec // small positive int
	specs := []kafkainfra.TopicSpec{
		{Name: kafkainfra.IngestTopic(prefix, kafkainfra.SignalLogs), Partitions: int32(cfg.KafkaPartitions("logs")), ReplicationFactor: rf},       //nolint:gosec
		{Name: kafkainfra.IngestTopic(prefix, kafkainfra.SignalSpans), Partitions: int32(cfg.KafkaPartitions("spans")), ReplicationFactor: rf},     //nolint:gosec
		{Name: kafkainfra.IngestTopic(prefix, kafkainfra.SignalMetrics), Partitions: int32(cfg.KafkaPartitions("metrics")), ReplicationFactor: rf}, //nolint:gosec
		{Name: kafkainfra.DLQTopic(prefix, kafkainfra.SignalLogs), Partitions: 4, ReplicationFactor: rf},
		{Name: kafkainfra.DLQTopic(prefix, kafkainfra.SignalSpans), Partitions: 4, ReplicationFactor: rf},
	}
	if err := kafkainfra.EnsureTopics(kafkaCfg.Brokers, specs); err != nil {
		return nil, fmt.Errorf("kafka ingest topics: %w", err)
	}

	ingest, producerClient, consumerClients, err := buildIngestModules(cfg, kafkaCfg, prefix, chConn)
	if err != nil {
		return nil, err
	}

	authenticator := auth.NewAuthenticator(dbConn, redisClients.Client)

	return &Infra{
		DB:              dbConn,
		CH:              chConn,
		SessionManager:  sessionManager,
		RedisClient:     redisClients.Client,
		RedisPool:       redisClients.Pool,
		Authenticator:   authenticator,
		Ingest:          ingest,
		producerClient:  producerClient,
		consumerClients: consumerClients,
	}, nil
}

func openClickHouse(cfg config.Config) (clickhouse.Conn, error) {
	chConn, err := dbutil.OpenClickHouseConn(cfg.ClickHouseDSN())
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	if cfg.ClickHouse.AutoMigrate {
		if err := runAutoMigrate(chConn, cfg.ClickHouse.Database); err != nil {
			_ = chConn.Close() //nolint:errcheck
			return nil, fmt.Errorf("clickhouse auto-migrate: %w", err)
		}
	}
	return chConn, nil
}

// runAutoMigrate applies pending DDL from the embedded `db/clickhouse/*.sql`
// fileset. Called only when `clickhouse.auto_migrate` is true in config —
// production deploys should prefer the explicit `./migrate up` CLI.
func runAutoMigrate(conn clickhouse.Conn, database string) error {
	m := &chmigrate.Migrator{
		DB:       conn,
		FS:       chembed.FS,
		Database: database,
		Logger:   func(format string, args ...any) { slog.Info(fmt.Sprintf("chmigrate: "+format, args...)) },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	applied, skipped, err := m.Up(ctx)
	if err != nil {
		return err
	}
	slog.Info("chmigrate: auto-migrate complete", slog.Int("applied", applied), slog.Int("skipped", skipped))
	return nil
}

// buildIngestModules constructs the shared producer + per-signal consumers +
// per-signal modules. The producer is a single kgo.Client shared across all
// three signals (one TCP pool); each consumer is its own client because they
// join different consumer groups.
func buildIngestModules(cfg config.Config, kcfg kafkainfra.Config, prefix string, ch clickhouse.Conn) (IngestModules, *kgo.Client, []*kgo.Client, error) {
	producerClient, err := kafkainfra.NewProducerClient(kcfg)
	if err != nil {
		return IngestModules{}, nil, nil, fmt.Errorf("kafka producer client: %w", err)
	}
	producer := kafkainfra.NewProducer(producerClient)

	consumerClients := make([]*kgo.Client, 0, 3)
	closeOnErr := func() {
		for _, c := range consumerClients {
			c.Close()
		}
		producerClient.Close()
	}

	logsPersist, err := newConsumer(kcfg, cfg.KafkaConsumerGroup(), prefix, kafkainfra.SignalLogs, "persistence")
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, err
	}
	consumerClients = append(consumerClients, logsPersist)

	metricsPersist, err := newConsumer(kcfg, cfg.KafkaConsumerGroup(), prefix, kafkainfra.SignalMetrics, "persistence")
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, err
	}
	consumerClients = append(consumerClients, metricsPersist)

	spansPersist, err := newConsumer(kcfg, cfg.KafkaConsumerGroup(), prefix, kafkainfra.SignalSpans, "persistence")
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, err
	}
	consumerClients = append(consumerClients, spansPersist)

	logsMod := ingestlogs.NewModule(ingestlogs.Deps{
		Producer:          ingestlogs.NewProducer(producer, prefix),
		CH:                ch,
		PersistenceClient: kafkainfra.NewConsumer(logsPersist),
		KafkaBase:         producer,
		TopicPrefix:       prefix,
	}).(*ingestlogs.Module)

	metricsMod := ingestmetrics.NewModule(ingestmetrics.Deps{
		Producer:          ingestmetrics.NewProducer(producer, prefix),
		CH:                ch,
		PersistenceClient: kafkainfra.NewConsumer(metricsPersist),
	}).(*ingestmetrics.Module)

	idxCfg := cfg.SpansIndexerConfig()
	spansMod := ingestspans.NewModule(ingestspans.Deps{
		Producer:          ingestspans.NewProducer(producer, prefix),
		CH:                ch,
		PersistenceClient: kafkainfra.NewConsumer(spansPersist),
		KafkaBase:         producer,
		TopicPrefix:       prefix,
		IndexerConfig: indexer.Config{
			Capacity:    idxCfg.Capacity,
			QuietWindow: time.Duration(idxCfg.QuietWindowMs) * time.Millisecond,
			HardTimeout: time.Duration(idxCfg.HardTimeoutMs) * time.Millisecond,
			SweepEvery:  time.Duration(idxCfg.SweepEveryMs) * time.Millisecond,
		},
	}).(*ingestspans.Module)

	return IngestModules{Logs: logsMod, Metrics: metricsMod, Spans: spansMod}, producerClient, consumerClients, nil
}

// newConsumer builds a Kafka consumer client for one signal + role pair.
// The role (e.g. "persistence") appears in the group id.
func newConsumer(kcfg kafkainfra.Config, groupBase, prefix, signal, role string) (*kgo.Client, error) {
	topic := kafkainfra.IngestTopic(prefix, signal)
	groupID := kafkainfra.GroupID(groupBase, signal, role)
	client, err := kafkainfra.NewConsumerClient(kcfg, groupID, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer %s/%s: %w", signal, role, err)
	}
	return client, nil
}

// Close releases every resource the Infra owns. Best-effort; errors are logged
// by callers but not returned — shutdown must proceed.
func (i *Infra) Close() error {
	if i == nil {
		return nil
	}
	// Consumers are closed by each module's Stop(); producer client is the
	// only Kafka resource Infra owns directly.
	if i.producerClient != nil {
		i.producerClient.Close()
	}
	if i.RedisPool != nil {
		_ = i.RedisPool.Close() //nolint:errcheck
	}
	if i.RedisClient != nil {
		_ = i.RedisClient.Close() //nolint:errcheck
	}
	if i.CH != nil {
		_ = i.CH.Close() //nolint:errcheck
	}
	if i.DB != nil {
		_ = i.DB.Close() //nolint:errcheck
	}
	return nil
}

func newSessionManager(cfg config.Config, pool *redigoredis.Pool) (session.Manager, error) {
	if pool == nil {
		return nil, fmt.Errorf("session manager requires redis pool")
	}
	return session.NewManager(cfg, pool)
}
