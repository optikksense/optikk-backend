package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	chembed "github.com/Optikk-Org/optikk-backend/db/clickhouse"
	"github.com/Optikk-Org/optikk-backend/internal/auth"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database_chmigrate"
	kclient "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/client"
	kconsumer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/consumer"
	kobserv "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/observability"
	kproducer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/producer"
	ktopics "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/topics"
	"github.com/Optikk-Org/optikk-backend/internal/infra/redis"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	logingress "github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/ingress"
	logmodule "github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/module"
	metringress "github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/ingress"
	metrmodule "github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/module"
	spaningress "github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/ingress"
	spanmodule "github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/module"
	indexer "github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/indexer"
	"github.com/twmb/franz-go/pkg/kgo"
	redigoredis "github.com/gomodule/redigo/redis"
	goredis "github.com/redis/go-redis/v9"
)

// IngestModules bundles the three ingestion modules returned to the registry.
// Held as a struct (instead of a slice) so modules_manifest.go can spread them
// in a readable order alongside non-ingest modules.
type IngestModules struct {
	Logs    *logmodule.Module
	Metrics *metrmodule.Module
	Spans   *spanmodule.Module
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
	// LagPollers publishes `optikk_kafka_consumer_lag_records` for each
	// ingest consumer group on a 15 s ticker. Started by app.Start as
	// run.Group actors so graceful shutdown kills them cleanly.
	LagPollers []*kobserv.LagPoller

	producerClient  *kgo.Client
	consumerClients []*kgo.Client // closed on shutdown alongside producer
}

func newInfra(cfg config.Config) (_ *Infra, err error) {
	dbConn, err := dbutil.Open(cfg.MySQLDSN(), cfg.MySQL.MaxOpenConns, cfg.MySQL.MaxIdleConns)
	if err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}
	slog.Info("mysql connected",
		slog.String("addr", net.JoinHostPort(cfg.MySQL.Host, cfg.MySQL.Port)),
		slog.String("database", cfg.MySQL.Database),
		slog.Int("max_open_conns", cfg.MySQL.MaxOpenConns),
	)
	defer func() {
		if err != nil {
			_ = dbConn.Close() //nolint:errcheck // cleanup after failed init
		}
	}()

	chConn, err := openClickHouse(cfg)
	if err != nil {
		return nil, err
	}
	slog.Info("clickhouse connected",
		slog.String("addr", net.JoinHostPort(cfg.ClickHouse.Host, cfg.ClickHouse.Port)),
		slog.String("database", cfg.ClickHouse.Database),
	)
	defer func() {
		if err != nil {
			_ = chConn.Close() //nolint:errcheck // cleanup after failed init
		}
	}()

	redisClients, err := redis.NewClients(cfg)
	if err != nil {
		return nil, err
	}
	slog.Info("redis connected",
		slog.String("addr", cfg.RedisAddr()),
		slog.Int("db", cfg.Redis.DB),
	)
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
	kafkaCfg := kclient.Config{Brokers: cfg.KafkaBrokers()}

	ingest, producerClient, consumerClients, lagPollers, err := buildIngestModules(cfg, kafkaCfg, prefix, chConn)
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
		LagPollers:      lagPollers,
		producerClient:  producerClient,
		consumerClients: consumerClients,
	}, nil
}

func openClickHouse(cfg config.Config) (clickhouse.Conn, error) {
	chConn, err := dbutil.OpenClickHouseConn(cfg.ClickHouseDSN())
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	if err := runMigrate(chConn, cfg.ClickHouse.Database); err != nil {
		_ = chConn.Close() //nolint:errcheck
		return nil, fmt.Errorf("clickhouse migrate: %w", err)
	}
	return chConn, nil
}

// runMigrate applies pending DDL from the embedded `db/clickhouse/*.sql`
// fileset. Always runs at server boot — schema is a hard prerequisite for
// serving traffic and the migrator is a no-op when there is nothing to apply.
func runMigrate(conn clickhouse.Conn, database string) error {
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
	slog.Info("chmigrate: complete", slog.Int("applied", applied), slog.Int("skipped", skipped))
	return nil
}

// buildIngestModules constructs the shared producer + per-signal consumers +
// per-signal modules. The producer is a single kgo.Client shared across all
// three signals (one TCP pool); each consumer is its own client because they
// join different consumer groups.
func buildIngestModules(cfg config.Config, kcfg kclient.Config, prefix string, ch clickhouse.Conn) (IngestModules, *kgo.Client, []*kgo.Client, []*kobserv.LagPoller, error) {
	producerClient, err := kclient.NewProducerClient(kcfg)
	if err != nil {
		return IngestModules{}, nil, nil, nil, fmt.Errorf("kafka producer client: %w", err)
	}
	slog.Info("kafka producer client connected", slog.Any("brokers", kcfg.Brokers))
	producer := kproducer.NewProducer(producerClient)

	consumerClients := make([]*kgo.Client, 0, 3)
	lagPollers := make([]*kobserv.LagPoller, 0, 3)
	closeOnErr := func() {
		for _, c := range consumerClients {
			c.Close()
		}
		producerClient.Close()
	}

	logsPersist, err := newConsumer(kcfg, cfg.KafkaConsumerGroup(), prefix, ktopics.SignalLogs, "persistence")
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, nil, err
	}
	consumerClients = append(consumerClients, logsPersist)
	lagPollers = append(lagPollers, kobserv.NewLagPoller(
		logsPersist,
		ktopics.GroupID(cfg.KafkaConsumerGroup(), ktopics.SignalLogs, "persistence"),
		ktopics.IngestTopic(prefix, ktopics.SignalLogs),
	))

	metricsPersist, err := newConsumer(kcfg, cfg.KafkaConsumerGroup(), prefix, ktopics.SignalMetrics, "persistence")
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, nil, err
	}
	consumerClients = append(consumerClients, metricsPersist)
	lagPollers = append(lagPollers, kobserv.NewLagPoller(
		metricsPersist,
		ktopics.GroupID(cfg.KafkaConsumerGroup(), ktopics.SignalMetrics, "persistence"),
		ktopics.IngestTopic(prefix, ktopics.SignalMetrics),
	))

	spansPersist, err := newConsumer(kcfg, cfg.KafkaConsumerGroup(), prefix, ktopics.SignalSpans, "persistence")
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, nil, err
	}
	consumerClients = append(consumerClients, spansPersist)
	lagPollers = append(lagPollers, kobserv.NewLagPoller(
		spansPersist,
		ktopics.GroupID(cfg.KafkaConsumerGroup(), ktopics.SignalSpans, "persistence"),
		ktopics.IngestTopic(prefix, ktopics.SignalSpans),
	))

	logsMod := logmodule.NewModule(logmodule.Deps{
		Producer:          logingress.NewProducer(producer, prefix),
		CH:                ch,
		PersistenceClient: kconsumer.NewConsumer(logsPersist),
		KafkaBase:         producer,
		TopicPrefix:       prefix,
		Pipeline:          cfg.IngestPipeline("logs"),
	}).(*logmodule.Module)

	metricsMod := metrmodule.NewModule(metrmodule.Deps{
		Producer:          metringress.NewProducer(producer, prefix),
		CH:                ch,
		PersistenceClient: kconsumer.NewConsumer(metricsPersist),
		KafkaBase:         producer,
		TopicPrefix:       prefix,
		Pipeline:          cfg.IngestPipeline("metrics"),
	}).(*metrmodule.Module)

	idxCfg := cfg.SpansIndexerConfig()
	spansMod := spanmodule.NewModule(spanmodule.Deps{
		Producer:          spaningress.NewProducer(producer, prefix),
		CH:                ch,
		PersistenceClient: kconsumer.NewConsumer(spansPersist),
		KafkaBase:         producer,
		TopicPrefix:       prefix,
		IndexerConfig: indexer.Config{
			Capacity:    idxCfg.Capacity,
			QuietWindow: time.Duration(idxCfg.QuietWindowMs) * time.Millisecond,
			HardTimeout: time.Duration(idxCfg.HardTimeoutMs) * time.Millisecond,
			SweepEvery:  time.Duration(idxCfg.SweepEveryMs) * time.Millisecond,
		},
		Pipeline: cfg.IngestPipeline("spans"),
	}).(*spanmodule.Module)

	return IngestModules{Logs: logsMod, Metrics: metricsMod, Spans: spansMod}, producerClient, consumerClients, lagPollers, nil
}

// newConsumer builds a Kafka consumer client for one signal + role pair.
// The role (e.g. "persistence") appears in the group id.
func newConsumer(kcfg kclient.Config, groupBase, prefix, signal, role string) (*kgo.Client, error) {
	topic := ktopics.IngestTopic(prefix, signal)
	groupID := ktopics.GroupID(groupBase, signal, role)
	client, err := kclient.NewConsumerClient(kcfg, groupID, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer %s/%s: %w", signal, role, err)
	}
	slog.Info("kafka consumer client connected",
		slog.String("signal", signal),
		slog.String("group", groupID),
		slog.String("topic", topic),
	)
	return client, nil
}

// Close releases every resource the Infra owns. Best-effort; errors are logged
// by callers but not returned — shutdown must proceed. Ordering mirrors the
// reverse of newInfra so consumers stop reading before the CH/producer sinks
// they feed go away.
func (i *Infra) Close() error {
	if i == nil {
		return nil
	}
	if n := len(i.consumerClients); n > 0 {
		for _, c := range i.consumerClients {
			c.Close()
		}
		slog.Info("kafka consumers closed", slog.Int("count", n))
	}
	if i.producerClient != nil {
		i.producerClient.Close()
		slog.Info("kafka producer closed")
	}
	if i.RedisPool != nil {
		_ = i.RedisPool.Close() //nolint:errcheck
	}
	if i.RedisClient != nil {
		_ = i.RedisClient.Close() //nolint:errcheck
		slog.Info("redis connection closed")
	}
	if i.CH != nil {
		_ = i.CH.Close() //nolint:errcheck
		slog.Info("clickhouse connection closed")
	}
	if i.DB != nil {
		_ = i.DB.Close() //nolint:errcheck
		slog.Info("mysql connection closed")
	}
	return nil
}

func newSessionManager(cfg config.Config, pool *redigoredis.Pool) (session.Manager, error) {
	if pool == nil {
		return nil, fmt.Errorf("session manager requires redis pool")
	}
	return session.NewManager(cfg, pool)
}
