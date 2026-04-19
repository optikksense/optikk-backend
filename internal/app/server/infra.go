package server

import (
	"database/sql"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/redis"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	ingestlogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/logs"
	ingestmetrics "github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics"
	ingestspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/spans"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
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
	LiveTailHub    livetail.Hub
	RedisClient    *goredis.Client
	RedisPool      *redigoredis.Pool
	Authenticator  *auth.Authenticator
	Ingest         IngestModules
	SketchQuerier  *sketch.Querier

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

	liveTailHub := livetail.NewHub(redisClients.Client, 0)

	prefix := cfg.KafkaTopicPrefix()
	kafkaCfg := kafkainfra.Config{Brokers: cfg.KafkaBrokers()}
	if err := kafkainfra.EnsureTopics(kafkaCfg.Brokers, kafkainfra.IngestTopics(prefix)); err != nil {
		return nil, fmt.Errorf("kafka ingest topics: %w", err)
	}

	sketchStore := sketch.NewRedisStore(redisClients.Client)
	sketchQuerier := sketch.NewQuerier(sketchStore, sketch.NewCHFallback(chConn))

	ingest, producerClient, consumerClients, err := buildIngestModules(cfg, kafkaCfg, prefix, chConn, liveTailHub, sketchStore)
	if err != nil {
		return nil, err
	}

	authenticator := auth.NewAuthenticator(dbConn, redisClients.Client)

	return &Infra{
		DB:              dbConn,
		CH:              chConn,
		SessionManager:  sessionManager,
		LiveTailHub:     liveTailHub,
		RedisClient:     redisClients.Client,
		RedisPool:       redisClients.Pool,
		Authenticator:   authenticator,
		Ingest:          ingest,
		SketchQuerier:   sketchQuerier,
		producerClient:  producerClient,
		consumerClients: consumerClients,
	}, nil
}

func openClickHouse(cfg config.Config) (clickhouse.Conn, error) {
	chConn, err := dbutil.OpenClickHouseConn(cfg.ClickHouseDSN())
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	return chConn, nil
}

// buildIngestModules constructs the shared producer + per-signal consumers +
// per-signal modules. The producer is a single kgo.Client shared across all
// three signals (one TCP pool); each consumer is its own client because they
// join different consumer groups.
func buildIngestModules(cfg config.Config, kcfg kafkainfra.Config, prefix string, ch clickhouse.Conn, hub livetail.Hub, store sketch.Store) (IngestModules, *kgo.Client, []*kgo.Client, error) {
	producerClient, err := kafkainfra.NewProducerClient(kcfg)
	if err != nil {
		return IngestModules{}, nil, nil, fmt.Errorf("kafka producer client: %w", err)
	}
	producer := kafkainfra.NewProducer(producerClient)

	consumerClients := make([]*kgo.Client, 0, 5)
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

	logsLive, err := newConsumer(kcfg, cfg.KafkaConsumerGroup(), prefix, kafkainfra.SignalLogs, "livetail")
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, err
	}
	consumerClients = append(consumerClients, logsLive)

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

	spansLive, err := newConsumer(kcfg, cfg.KafkaConsumerGroup(), prefix, kafkainfra.SignalSpans, "livetail")
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, err
	}
	consumerClients = append(consumerClients, spansLive)

	logsMod := ingestlogs.NewModule(ingestlogs.Deps{
		Producer:          ingestlogs.NewProducer(producer, prefix),
		CH:                ch,
		PersistenceClient: kafkainfra.NewConsumer(logsPersist),
		LivetailClient:    kafkainfra.NewConsumer(logsLive),
		Hub:               hub,
	}).(*ingestlogs.Module)

	metricsMod := ingestmetrics.NewModule(ingestmetrics.Deps{
		Producer:          ingestmetrics.NewProducer(producer, prefix),
		CH:                ch,
		PersistenceClient: kafkainfra.NewConsumer(metricsPersist),
		SketchStore:       store,
	}).(*ingestmetrics.Module)

	spansMod := ingestspans.NewModule(ingestspans.Deps{
		Producer:          ingestspans.NewProducer(producer, prefix),
		CH:                ch,
		PersistenceClient: kafkainfra.NewConsumer(spansPersist),
		LivetailClient:    kafkainfra.NewConsumer(spansLive),
		Hub:               hub,
		SketchStore:       store,
	}).(*ingestspans.Module)

	return IngestModules{Logs: logsMod, Metrics: metricsMod, Spans: spansMod}, producerClient, consumerClients, nil
}

// newConsumer builds a Kafka consumer client for one signal + role pair.
// The role ("persistence" | "livetail") appears in the group id so each role
// tracks offsets independently.
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
