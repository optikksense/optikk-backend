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

	ClickHouseHost       string
	ClickHousePort       string
	ClickHouseDatabase   string
	ClickHouseUser       string
	ClickHousePassword   string
	ClickHouseProduction bool
	// ClickHouseCloudHost is the production ClickHouse Cloud address (host:port).
	// Only used when ClickHouseProduction is true.
	ClickHouseCloudHost string

	JWTSecret       string
	JWTExpirationMs int64

	QueueBatchSize       int
	QueueFlushIntervalMs int64

	KafkaEnabled bool
	KafkaBrokers string

	RedisEnabled bool
	RedisHost    string
	RedisPort    string

	AllowedOrigins string

	GRPCPort             string
	HTTPPortOTLP         string
	GRPCMaxRecvMsgSizeMB int

	MaxMySQLOpenConns int
	MaxMySQLIdleConns int

	DefaultRetentionDays int

	// AppRegion is the Kubernetes region/AZ this pod is running in (e.g. "us-east-1").
	// Populated from APP_REGION env var (set in the K8s Deployment manifest).
	// Used to scope ClickHouse queries to the correct shard.
	AppRegion string

	// OAuth providers (Google + GitHub).
	// Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET to enable Google sign-in.
	// Set GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET to enable GitHub sign-in.
	// OAUTH_REDIRECT_BASE is the frontend base URL used to build callback redirect URLs.
	GoogleClientID     string
	GoogleClientSecret string
	GitHubClientID     string
	GitHubClientSecret string
	OAuthRedirectBase  string

	DebugAPILogs bool
}

// Load reads configuration from environment variables.
// In production (GO_ENV=production), JWT_SECRET, MYSQL_PASSWORD, and
// CLICKHOUSE_PASSWORD must be set explicitly — the process will exit
// if they are left at their insecure defaults.
func Load() Config {
	cfg := Config{
		Port:                 getEnv("PORT", "9090"),
		MySQLHost:            getEnv("MYSQL_HOST", "127.0.0.1"),
		MySQLPort:            getEnv("MYSQL_PORT", "3306"),
		MySQLDatabase:        getEnv("MYSQL_DATABASE", "observability"),
		MySQLUser:            getEnv("MYSQL_USERNAME", "root"),
		MySQLPassword:        getEnv("MYSQL_PASSWORD", "root123"),
		ClickHouseHost:       getEnv("CLICKHOUSE_HOST", "127.0.0.1"),
		ClickHousePort:       getEnv("CLICKHOUSE_PORT", "9000"),
		ClickHouseDatabase:   getEnv("CLICKHOUSE_DATABASE", "observability"),
		ClickHouseUser:       getEnv("CLICKHOUSE_USERNAME", "default"),
		ClickHousePassword:   getEnv("CLICKHOUSE_PASSWORD", "clickhouse123"),
		ClickHouseProduction: getEnvBool("CLICKHOUSE_PRODUCTION", false),
		ClickHouseCloudHost:  getEnv("CLICKHOUSE_CLOUD_HOST", ""),
		JWTSecret:            getEnv("JWT_SECRET", "optic-secret-key-for-jwt-token-generation-must-be-at-least-256-bits"),
		JWTExpirationMs:      getEnvInt64("JWT_EXPIRATION_MS", 86_400_000),

		QueueBatchSize:       int(getEnvInt64("QUEUE_BATCH_SIZE", 1000)),
		QueueFlushIntervalMs: getEnvInt64("QUEUE_FLUSH_INTERVAL_MS", 2_000),

		KafkaEnabled: getEnvBool("KAFKA_ENABLED", false),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),

		RedisEnabled: getEnvBool("REDIS_ENABLED", false),
		RedisHost:    getEnv("REDIS_HOST", "localhost"),
		RedisPort:    getEnv("REDIS_PORT", "6379"),

		AllowedOrigins: getEnv("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:5173"),

		GRPCPort:             getEnv("GRPC_PORT", "4317"),
		HTTPPortOTLP:         getEnv("HTTP_PORT_OTLP", "4318"),
		GRPCMaxRecvMsgSizeMB: int(getEnvInt64("GRPC_MAX_RECV_MSG_SIZE_MB", 16)),

		MaxMySQLOpenConns: int(getEnvInt64("MAX_MYSQL_OPEN_CONNS", 50)),
		MaxMySQLIdleConns: int(getEnvInt64("MAX_MYSQL_IDLE_CONNS", 25)),

		DefaultRetentionDays: int(getEnvInt64("DEFAULT_RETENTION_DAYS", 30)),

		AppRegion: getEnv("APP_REGION", "us-east-1"),

		GoogleClientID:     getEnv("GOOGLE_CLIENT_ID", ""),
		GoogleClientSecret: getEnv("GOOGLE_CLIENT_SECRET", ""),
		GitHubClientID:     getEnv("GITHUB_CLIENT_ID", ""),
		GitHubClientSecret: getEnv("GITHUB_CLIENT_SECRET", ""),
		OAuthRedirectBase:  getEnv("OAUTH_REDIRECT_BASE", "http://localhost:3000"),

		DebugAPILogs: getEnvBool("DEBUG_API_LOGS", true),
	}

	cfg.validate()
	return cfg
}

func (c Config) validate() {
	isProd := strings.EqualFold(os.Getenv("GO_ENV"), "production")
	if !isProd {
		return
	}
	var errs []string
	if c.JWTSecret == "optic-secret-key-for-jwt-token-generation-must-be-at-least-256-bits" {
		errs = append(errs, "JWT_SECRET must be set in production (do not use the default)")
	}
	if c.MySQLPassword == "root123" {
		errs = append(errs, "MYSQL_PASSWORD must be set in production (do not use the default)")
	}
	if c.ClickHousePassword == "clickhouse123" {
		errs = append(errs, "CLICKHOUSE_PASSWORD must be set in production (do not use the default)")
	}
	if c.ClickHouseProduction && c.ClickHouseCloudHost == "" {
		errs = append(errs, "CLICKHOUSE_CLOUD_HOST must be set when CLICKHOUSE_PRODUCTION is true")
	}
	if len(errs) > 0 {
		fmt.Fprintf(os.Stderr, "FATAL: insecure configuration detected:\n")
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "  - %s\n", e)
		}
		os.Exit(1)
	}
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
