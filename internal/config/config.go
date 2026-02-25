package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port string

	MySQLHost     string
	MySQLPort     string
	MySQLDatabase string
	MySQLUser     string
	MySQLPassword string

	ClickHouseHost     string
	ClickHousePort     string
	ClickHouseDatabase string
	ClickHouseUser     string
	ClickHousePassword string

	JWTSecret       string
	JWTExpirationMs int64

	QueueBatchSize       int
	QueueFlushIntervalMs int64

	KafkaEnabled bool
	KafkaBrokers string
}

func Load() Config {
	cfg := Config{
		Port:               getEnv("PORT", "8080"),
		MySQLHost:          getEnv("MYSQL_HOST", "127.0.0.1"),
		MySQLPort:          getEnv("MYSQL_PORT", "3306"),
		MySQLDatabase:      getEnv("MYSQL_DATABASE", "observability"),
		MySQLUser:          getEnv("MYSQL_USERNAME", "root"),
		MySQLPassword:      getEnv("MYSQL_PASSWORD", "root123"),
		ClickHouseHost:     getEnv("CLICKHOUSE_HOST", "127.0.0.1"),
		ClickHousePort:     getEnv("CLICKHOUSE_PORT", "9000"),
		ClickHouseDatabase: getEnv("CLICKHOUSE_DATABASE", "observability"),
		ClickHouseUser:     getEnv("CLICKHOUSE_USERNAME", "default"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", "clickhouse123"),
		JWTSecret:          getEnv("JWT_SECRET", "observex-secret-key-for-jwt-token-generation-must-be-at-least-256-bits"),
		JWTExpirationMs:    getEnvInt64("JWT_EXPIRATION_MS", 86_400_000),

		QueueBatchSize:       int(getEnvInt64("QUEUE_BATCH_SIZE", 500)),
		QueueFlushIntervalMs: getEnvInt64("QUEUE_FLUSH_INTERVAL_MS", 2_000),

		KafkaEnabled: getEnvBool("KAFKA_ENABLED", true),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
	}

	return cfg
}

func (c Config) MySQLDSN() string {
	// multiStatements is intentionally omitted: allowing multiple statements
	// per Exec call dramatically amplifies the impact of any SQL injection.
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4&loc=UTC",
		c.MySQLUser,
		c.MySQLPassword,
		c.MySQLHost,
		c.MySQLPort,
		c.MySQLDatabase,
	)
}

func (c Config) ClickHouseDSN() string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s",
		c.ClickHouseUser,
		c.ClickHousePassword,
		c.ClickHouseHost,
		c.ClickHousePort,
		c.ClickHouseDatabase,
	)
}

func (c Config) JWTDuration() time.Duration {
	return time.Duration(c.JWTExpirationMs) * time.Millisecond
}

func (c Config) QueueFlushInterval() time.Duration {
	return time.Duration(c.QueueFlushIntervalMs) * time.Millisecond
}

func (c Config) KafkaBrokerList() []string {
	return strings.Split(c.KafkaBrokers, ",")
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return strings.EqualFold(v, "true") || v == "1"
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}
