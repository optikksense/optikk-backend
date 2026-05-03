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
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/redis"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	logsignal "github.com/Optikk-Org/optikk-backend/internal/ingestion/logs"
	metricsignal "github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics"
	spansignal "github.com/Optikk-Org/optikk-backend/internal/ingestion/spans"
	redigoredis "github.com/gomodule/redigo/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

// IngestModules bundles the three ingestion modules returned to the registry.
type IngestModules struct {
	Logs    *logsignal.Module
	Metrics *metricsignal.Module
	Spans   *spansignal.Module
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
	LagPollers     []*kafkainfra.LagPoller

	producerClient  *kgo.Client
	consumerClients []*kgo.Client
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
			_ = dbConn.Close() //nolint:errcheck
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
			_ = chConn.Close() //nolint:errcheck
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

	ingest, producerClient, consumerClients, lagPollers, err := buildIngest(cfg, chConn)
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

func runMigrate(conn clickhouse.Conn, database string) error {
	m := &dbutil.Migrator{
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

// buildIngest creates topics, opens one shared producer client + per-signal
// consumer clients, and constructs the three signal modules. Returns the
// modules bundle, the producer client (closed at shutdown), the consumer
// clients (closed at shutdown), and the lag pollers (run by the run.Group).
func buildIngest(cfg config.Config, ch clickhouse.Conn) (IngestModules, *kgo.Client, []*kgo.Client, []*kafkainfra.LagPoller, error) {
	brokers := cfg.KafkaBrokers()
	topicPrefix := cfg.KafkaTopicPrefix()
	dlqPrefix := cfg.KafkaDLQPrefix()

	spans := cfg.IngestSignal("spans")
	logs := cfg.IngestSignal("logs")
	metrics := cfg.IngestSignal("metrics")

	specs := []kafkainfra.TopicSpec{
		{Name: kafkainfra.IngestTopic(topicPrefix, kafkainfra.SignalSpans), Partitions: int32(spans.Partitions), Replicas: int16(spans.Replicas), RetentionHours: spans.RetentionHours},
		{Name: kafkainfra.DLQTopic(dlqPrefix, kafkainfra.SignalSpans), Partitions: int32(spans.Partitions), Replicas: int16(spans.Replicas), RetentionHours: spans.RetentionHours},
		{Name: kafkainfra.IngestTopic(topicPrefix, kafkainfra.SignalLogs), Partitions: int32(logs.Partitions), Replicas: int16(logs.Replicas), RetentionHours: logs.RetentionHours},
		{Name: kafkainfra.DLQTopic(dlqPrefix, kafkainfra.SignalLogs), Partitions: int32(logs.Partitions), Replicas: int16(logs.Replicas), RetentionHours: logs.RetentionHours},
		{Name: kafkainfra.IngestTopic(topicPrefix, kafkainfra.SignalMetrics), Partitions: int32(metrics.Partitions), Replicas: int16(metrics.Replicas), RetentionHours: metrics.RetentionHours},
		{Name: kafkainfra.DLQTopic(dlqPrefix, kafkainfra.SignalMetrics), Partitions: int32(metrics.Partitions), Replicas: int16(metrics.Replicas), RetentionHours: metrics.RetentionHours},
	}
	ensureCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := kafkainfra.EnsureTopics(ensureCtx, brokers, specs); err != nil {
		return IngestModules{}, nil, nil, nil, err
	}

	kcfg := kafkainfra.Config{
		Brokers:       brokers,
		LingerMs:      cfg.KafkaLingerMs(),
		BatchMaxBytes: cfg.KafkaBatchMaxBytes(),
		Compression:   cfg.KafkaCompression(),
	}

	producerClient, err := kafkainfra.NewProducerClient(kcfg)
	if err != nil {
		return IngestModules{}, nil, nil, nil, fmt.Errorf("kafka producer client: %w", err)
	}
	slog.Info("kafka producer client connected", slog.Any("brokers", brokers))
	producerBase := kafkainfra.NewProducer(producerClient)

	consumerClients := make([]*kgo.Client, 0, 3)
	lagPollers := make([]*kafkainfra.LagPoller, 0, 3)
	closeOnErr := func() {
		for _, c := range consumerClients {
			c.Close()
		}
		producerClient.Close()
	}

	// spans signal
	spansTopic := kafkainfra.IngestTopic(topicPrefix, kafkainfra.SignalSpans)
	spansClient, err := kafkainfra.NewConsumerClient(kcfg, spans.ConsumerGroup, spansTopic)
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, nil, fmt.Errorf("kafka spans consumer: %w", err)
	}
	consumerClients = append(consumerClients, spansClient)
	lagPollers = append(lagPollers, kafkainfra.NewLagPoller(spansClient, spans.ConsumerGroup, spansTopic))

	spansProducer := spansignal.NewProducer(spansignal.ProducerConfig{
		Topic:          spansTopic,
		Partitions:     int32(spans.Partitions),
		Replicas:       int16(spans.Replicas),
		RetentionHours: spans.RetentionHours,
	}, producerBase)
	spansWriter := spansignal.NewWriter(ch)
	spansDLQ := spansignal.NewDLQ(producerBase, kafkainfra.DLQTopic(dlqPrefix, kafkainfra.SignalSpans))
	spansConsumer := spansignal.NewConsumer(spansignal.ConsumerConfig{
		Topic:         spansTopic,
		ConsumerGroup: spans.ConsumerGroup,
	}, kafkainfra.NewConsumer(spansClient), spansWriter, spansDLQ)
	spansMod := spansignal.NewModule(spansignal.Deps{
		Handler:  spansignal.NewHandler(spansProducer),
		Consumer: spansConsumer,
	}).(*spansignal.Module)

	// logs signal
	logsTopic := kafkainfra.IngestTopic(topicPrefix, kafkainfra.SignalLogs)
	logsClient, err := kafkainfra.NewConsumerClient(kcfg, logs.ConsumerGroup, logsTopic)
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, nil, fmt.Errorf("kafka logs consumer: %w", err)
	}
	consumerClients = append(consumerClients, logsClient)
	lagPollers = append(lagPollers, kafkainfra.NewLagPoller(logsClient, logs.ConsumerGroup, logsTopic))

	logsProducer := logsignal.NewProducer(logsignal.ProducerConfig{
		Topic:          logsTopic,
		Partitions:     int32(logs.Partitions),
		Replicas:       int16(logs.Replicas),
		RetentionHours: logs.RetentionHours,
	}, producerBase)
	logsWriter := logsignal.NewWriter(ch)
	logsDLQ := logsignal.NewDLQ(producerBase, kafkainfra.DLQTopic(dlqPrefix, kafkainfra.SignalLogs))
	logsConsumer := logsignal.NewConsumer(logsignal.ConsumerConfig{
		Topic:         logsTopic,
		ConsumerGroup: logs.ConsumerGroup,
	}, kafkainfra.NewConsumer(logsClient), logsWriter, logsDLQ)
	logsMod := logsignal.NewModule(logsignal.Deps{
		Handler:  logsignal.NewHandler(logsProducer),
		Consumer: logsConsumer,
	}).(*logsignal.Module)

	// metrics signal
	metricsTopic := kafkainfra.IngestTopic(topicPrefix, kafkainfra.SignalMetrics)
	metricsClient, err := kafkainfra.NewConsumerClient(kcfg, metrics.ConsumerGroup, metricsTopic)
	if err != nil {
		closeOnErr()
		return IngestModules{}, nil, nil, nil, fmt.Errorf("kafka metrics consumer: %w", err)
	}
	consumerClients = append(consumerClients, metricsClient)
	lagPollers = append(lagPollers, kafkainfra.NewLagPoller(metricsClient, metrics.ConsumerGroup, metricsTopic))

	metricsProducer := metricsignal.NewProducer(metricsignal.ProducerConfig{
		Topic:          metricsTopic,
		Partitions:     int32(metrics.Partitions),
		Replicas:       int16(metrics.Replicas),
		RetentionHours: metrics.RetentionHours,
	}, producerBase)
	metricsWriter := metricsignal.NewWriter(ch)
	metricsDLQ := metricsignal.NewDLQ(producerBase, kafkainfra.DLQTopic(dlqPrefix, kafkainfra.SignalMetrics))
	metricsConsumer := metricsignal.NewConsumer(metricsignal.ConsumerConfig{
		Topic:         metricsTopic,
		ConsumerGroup: metrics.ConsumerGroup,
	}, kafkainfra.NewConsumer(metricsClient), metricsWriter, metricsDLQ)
	metricsMod := metricsignal.NewModule(metricsignal.Deps{
		Handler:  metricsignal.NewHandler(metricsProducer),
		Consumer: metricsConsumer,
	}).(*metricsignal.Module)

	return IngestModules{Logs: logsMod, Metrics: metricsMod, Spans: spansMod}, producerClient, consumerClients, lagPollers, nil
}

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
