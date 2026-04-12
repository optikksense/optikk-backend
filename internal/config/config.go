package config

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

type ServerConfig struct {
	Port           string `yaml:"port"`
	AllowedOrigins string `yaml:"allowed_origins"`
	DebugAPILogs   bool   `yaml:"debug_api_logs"`
}

type MySQLConfig struct {
	Host         string `yaml:"host"`
	Port         string `yaml:"port"`
	Database     string `yaml:"database"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

type ClickHouseConfig struct {
	Host       string `yaml:"host"`
	Port       string `yaml:"port"`
	Database   string `yaml:"database"`
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	Production bool   `yaml:"production"`
	CloudHost  string `yaml:"cloud_host"`
}

type SessionConfig struct {
	LifetimeMs     int64  `yaml:"lifetime_ms"`
	IdleTimeoutMs  int64  `yaml:"idle_timeout_ms"`
	CookieName     string `yaml:"cookie_name"`
	CookieDomain   string `yaml:"cookie_domain"`
	CookiePath     string `yaml:"cookie_path"`
	CookieSecure   bool   `yaml:"cookie_secure"`
	CookieHTTPOnly bool   `yaml:"cookie_http_only"`
	CookieSameSite string `yaml:"cookie_same_site"`
}

// OtlRedisStream configures Redis Streams OTLP ingest (XADD + background consumers).
type OtlRedisStream struct {
	// MaxLenApprox is approximate MAXLEN per ingest stream (spans and metrics).
	MaxLenApprox int64 `yaml:"max_len_approx"`
	// LogsMaxLenApprox is approximate MAXLEN specific to the logs ingest stream.
	LogsMaxLenApprox int64 `yaml:"logs_max_len_approx"`
	// StreamTTLSeconds is the maximum age of stream entries (using MINID ~ pruning if Redis 6.2+).
	StreamTTLSeconds int64 `yaml:"stream_ttl_seconds"`
	// ChBatchSize is max rows per ClickHouse flush from stream reads.
	ChBatchSize int `yaml:"ch_batch_size"`
	// ChFlushIntervalMs bounds how long the CH consumer waits before flushing a partial batch.
	ChFlushIntervalMs int64 `yaml:"ch_flush_interval_ms"`
	// XReadBlockMs is BLOCK timeout for XREADGROUP (consumer wait).
	XReadBlockMs int64 `yaml:"xread_block_ms"`
	// XReadCount is COUNT for each XREADGROUP batch.
	XReadCount int64 `yaml:"xread_count"`
}

type RedisConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Password string `yaml:"password"`
	// DB selects the Redis logical database index (go-redis / redigo). Default 0.
	DB int `yaml:"db"`
}

type OTLPConfig struct {
	GRPCPort string `yaml:"grpc_port"`
}

type RetentionConfig struct {
	DefaultDays int `yaml:"default_days"`
}

type AppConfig struct {
	Region string `yaml:"region"`
}

type PlatformProvidersConfig struct {
	Session             string `yaml:"session"`
	LiveTailHub         string `yaml:"live_tail_hub"`
	IngestionDispatcher string `yaml:"ingestion_dispatcher"`
}

type PlatformConfig struct {
	Providers PlatformProvidersConfig `yaml:"providers"`
}

type IngestionConfig struct {
	SpansBucketSeconds int64 `yaml:"spans_bucket_seconds"`
	LogsBucketSeconds  int64 `yaml:"logs_bucket_seconds"`
	// QueueSize is the buffer size for the in-memory Go channels before ClickHouse ingestion.
	QueueSize int `yaml:"queue_size"`
	// ByteTrackerFlushIntervalMs is the frequency at which byte tracking totals are flushed to MySQL.
	ByteTrackerFlushIntervalMs int64 `yaml:"byte_tracker_flush_interval_ms"`
	// BatchMaxRows is the maximum number of rows to buffer before flushing to ClickHouse.
	BatchMaxRows int `yaml:"batch_max_rows"`
	// BatchMaxWaitMs is the maximum time in milliseconds to wait before flushing a partial batch to ClickHouse.
	BatchMaxWaitMs int64 `yaml:"batch_max_wait_ms"`
}

type AlertingConfig struct {
	// MaxEnabledRules is the safety cap on the number of enabled alert rules loaded per evaluation cycle.
	MaxEnabledRules int `yaml:"max_enabled_rules"`
}

type Config struct {
	Environment    string           `yaml:"environment"`
	Server         ServerConfig     `yaml:"server"`
	MySQL          MySQLConfig      `yaml:"mysql"`
	ClickHouse     ClickHouseConfig `yaml:"clickhouse"`
	Session        SessionConfig    `yaml:"session"`
	OtlRedisStream OtlRedisStream   `yaml:"otl_redis_stream"`
	Redis          RedisConfig      `yaml:"redis"`
	OTLP           OTLPConfig       `yaml:"otlp"`
	Retention      RetentionConfig  `yaml:"retention"`
	App            AppConfig        `yaml:"app"`
	Platform       PlatformConfig   `yaml:"platform"`
	Ingestion      IngestionConfig  `yaml:"ingestion"`
	Alerting       AlertingConfig   `yaml:"alerting"`
}

// Load reads configuration from a YAML file with environment variable overrides.
// If no path is provided, it defaults to "config.yml".
// Environment variables use the OPTIKK_ prefix with dots replaced by underscores
// (e.g., mysql.host → OPTIKK_MYSQL_HOST).
// In production (environment: production), passwords must not use insecure defaults.
func Load(path ...string) (Config, error) {
	p := "config.yml"
	if len(path) > 0 && path[0] != "" {
		p = path[0]
	}

	v := viper.New()
	v.SetConfigFile(p)

	setDefaults(v)

	v.SetEnvPrefix("OPTIKK")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("cannot read config file %s: %w", p, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecoderConfigOption(func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "yaml"
	})); err != nil {
		return Config{}, fmt.Errorf("invalid config in %s: %w", p, err)
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// setDefaults registers all known config keys with Viper so that
// AutomaticEnv() can resolve environment variable overrides even for
// keys that are absent from the YAML file.
func setDefaults(v *viper.Viper) {
	// top-level
	v.SetDefault("environment", "")

	// server
	v.SetDefault("server.port", "")
	v.SetDefault("server.allowed_origins", "")
	v.SetDefault("server.debug_api_logs", false)

	// mysql
	v.SetDefault("mysql.host", "")
	v.SetDefault("mysql.port", "")
	v.SetDefault("mysql.database", "")
	v.SetDefault("mysql.user", "")
	v.SetDefault("mysql.password", "")
	v.SetDefault("mysql.max_open_conns", 0)
	v.SetDefault("mysql.max_idle_conns", 0)

	// clickhouse
	v.SetDefault("clickhouse.host", "")
	v.SetDefault("clickhouse.port", "")
	v.SetDefault("clickhouse.database", "")
	v.SetDefault("clickhouse.user", "")
	v.SetDefault("clickhouse.password", "")
	v.SetDefault("clickhouse.production", false)
	v.SetDefault("clickhouse.cloud_host", "")

	// session
	v.SetDefault("session.lifetime_ms", 0)
	v.SetDefault("session.idle_timeout_ms", 0)
	v.SetDefault("session.cookie_name", "")
	v.SetDefault("session.cookie_domain", "")
	v.SetDefault("session.cookie_path", "")
	v.SetDefault("session.cookie_secure", false)
	v.SetDefault("session.cookie_http_only", false)
	v.SetDefault("session.cookie_same_site", "")

	// otl_redis_stream
	v.SetDefault("otl_redis_stream.max_len_approx", 0)
	v.SetDefault("otl_redis_stream.logs_max_len_approx", 0)
	v.SetDefault("otl_redis_stream.stream_ttl_seconds", 0)
	v.SetDefault("otl_redis_stream.ch_batch_size", 0)
	v.SetDefault("otl_redis_stream.ch_flush_interval_ms", 0)
	v.SetDefault("otl_redis_stream.xread_block_ms", 0)
	v.SetDefault("otl_redis_stream.xread_count", 0)

	// redis
	v.SetDefault("redis.enabled", false)
	v.SetDefault("redis.host", "")
	v.SetDefault("redis.port", "")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	// otlp
	v.SetDefault("otlp.grpc_port", "")

	// retention
	v.SetDefault("retention.default_days", 0)

	// app
	v.SetDefault("app.region", "")

	// platform
	v.SetDefault("platform.providers.session", "")
	v.SetDefault("platform.providers.live_tail_hub", "")
	v.SetDefault("platform.providers.ingestion_dispatcher", "")

	// ingestion
	v.SetDefault("ingestion.spans_bucket_seconds", 0)
	v.SetDefault("ingestion.logs_bucket_seconds", 0)
	v.SetDefault("ingestion.queue_size", 0)
	v.SetDefault("ingestion.byte_tracker_flush_interval_ms", 0)
	v.SetDefault("ingestion.batch_max_rows", 0)
	v.SetDefault("ingestion.batch_max_wait_ms", 0)

	// alerting
	v.SetDefault("alerting.max_enabled_rules", 0)
}

func (c Config) validate() error {
	isProd := strings.EqualFold(c.Environment, "production")
	if !isProd {
		if c.Redis.Enabled {
			return c.validateRedis()
		}
		return nil
	}
	var errs []string
	if c.MySQL.Password == "root123" {
		errs = append(errs, "mysql.password must be set in production")
	}
	if c.ClickHouse.Password == "clickhouse123" {
		errs = append(errs, "clickhouse.password must be set in production")
	}
	if c.ClickHouse.Production && c.ClickHouse.CloudHost == "" {
		errs = append(errs, "clickhouse.cloud_host must be set when clickhouse.production is true")
	}
	if !c.Redis.Enabled {
		errs = append(errs, "redis.enabled must be true in production")
	} else if err := c.validateRedis(); err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("insecure configuration detected: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (c Config) validateRedis() error {
	var errs []string
	if strings.TrimSpace(c.Redis.Host) == "" {
		errs = append(errs, "redis.host must be set")
	}
	if strings.TrimSpace(c.Redis.Port) == "" {
		errs = append(errs, "redis.port must be set")
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func (c Config) MySQLDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4&loc=UTC",
		c.MySQL.User,
		c.MySQL.Password,
		c.MySQL.Host,
		c.MySQL.Port,
		c.MySQL.Database,
	)
}

func (c Config) ClickHouseDSN() string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s",
		c.ClickHouse.User,
		c.ClickHouse.Password,
		c.ClickHouse.Host,
		c.ClickHouse.Port,
		c.ClickHouse.Database,
	)
}

func (c Config) RedisAddr() string {
	return net.JoinHostPort(c.Redis.Host, c.Redis.Port)
}

func (c Config) SessionLifetime() time.Duration {
	return time.Duration(c.Session.LifetimeMs) * time.Millisecond
}

func (c Config) SessionIdleTimeout() time.Duration {
	return time.Duration(c.Session.IdleTimeoutMs) * time.Millisecond
}

func (c Config) OtlChFlushInterval() time.Duration {
	ms := c.OtlRedisStream.ChFlushIntervalMs
	if ms <= 0 {
		return 2 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}

func (c Config) OtlXReadBlock() time.Duration {
	ms := c.OtlRedisStream.XReadBlockMs
	if ms <= 0 {
		return 2 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}

func (c Config) SpansBucketSeconds() int64 {
	if c.Ingestion.SpansBucketSeconds <= 0 {
		return 300 // 5 minutes
	}
	return c.Ingestion.SpansBucketSeconds
}

func (c Config) LogsBucketSeconds() int64 {
	if c.Ingestion.LogsBucketSeconds <= 0 {
		return 86400 // 1 day
	}
	return c.Ingestion.LogsBucketSeconds
}

func (c Config) IngestionQueueSize() int {
	n := c.Ingestion.QueueSize
	if n <= 0 {
		return 10000
	}
	return n
}

func (c Config) ByteTrackerFlushInterval() time.Duration {
	ms := c.Ingestion.ByteTrackerFlushIntervalMs
	if ms <= 0 {
		return 5 * time.Minute
	}
	return time.Duration(ms) * time.Millisecond
}

func (c Config) OtlLogsMaxLen() int64 {
	if c.OtlRedisStream.MaxLenApprox <= 0 {
		return 5000 // default for logs
	}
	return c.OtlRedisStream.MaxLenApprox
}

func (c Config) OtlSpansMaxLen() int64 {
	if c.OtlRedisStream.MaxLenApprox <= 0 {
		return 1000 // default for spans
	}
	return c.OtlRedisStream.MaxLenApprox
}

func (c Config) OtlStreamTTL() time.Duration {
	n := c.OtlRedisStream.StreamTTLSeconds
	if n <= 0 {
		return 3600 * time.Second // 1 hour
	}
	return time.Duration(n) * time.Second
}

func (c Config) SessionProvider() string {
	fallback := "local"
	if c.Redis.Enabled {
		fallback = "redis"
	}
	return firstNonEmpty(c.Platform.Providers.Session, fallback)
}


func (c Config) LiveTailHubProvider() string {
	return firstNonEmpty(c.Platform.Providers.LiveTailHub, "local")
}

func (c Config) IngestionBatchMaxRows() int {
	n := c.Ingestion.BatchMaxRows
	if n <= 0 {
		return 5000
	}
	return n
}

func (c Config) IngestionBatchMaxWait() time.Duration {
	ms := c.Ingestion.BatchMaxWaitMs
	if ms <= 0 {
		return 5 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}

func (c Config) AlertingMaxEnabledRules() int {
	n := c.Alerting.MaxEnabledRules
	if n <= 0 {
		return 10000
	}
	return n
}

func (c Config) IngestionDispatcherProvider() string {
	return firstNonEmpty(c.Platform.Providers.IngestionDispatcher, "local")
}

func firstNonEmpty(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
