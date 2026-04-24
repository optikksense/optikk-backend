package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

type Config struct {
	Environment    string           `yaml:"environment"`
	Server         ServerConfig     `yaml:"server"`
	MySQL          MySQLConfig      `yaml:"mysql"`
	ClickHouse     ClickHouseConfig `yaml:"clickhouse"`
	Session        SessionConfig    `yaml:"session"`
	OtlRedisStream OtlRedisStream   `yaml:"otl_redis_stream"`
	Redis          RedisConfig      `yaml:"redis"`
	Kafka          KafkaConfig      `yaml:"kafka"`
	OTLP           OTLPConfig       `yaml:"otlp"`
	Retention      RetentionConfig  `yaml:"retention"`
	App            AppConfig        `yaml:"app"`
	Ingestion      IngestionConfig  `yaml:"ingestion"`
}

// Load reads configuration from a YAML file with environment variable overrides.
// If no path is provided, it defaults to "config.yml".
// Relative paths are resolved against the current working directory, then each parent
// directory up to the filesystem root (so e.g. debugging with cwd cmd/server still finds
// the repo-root config.yml).
// Environment variables use the OPTIKK_ prefix with dots replaced by underscores
// (e.g., mysql.host → OPTIKK_MYSQL_HOST).
// In production (environment: production), passwords must not use insecure defaults.
func Load(path ...string) (Config, error) {
	p := "config.yml"
	if len(path) > 0 && path[0] != "" {
		p = path[0]
	}

	resolved, err := resolveConfigFilePath(p)
	if err != nil {
		return Config{}, err
	}

	v := viper.New()
	v.SetConfigFile(resolved)

	setDefaults(v)

	v.SetEnvPrefix("OPTIKK")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("cannot read config file %s: %w", resolved, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecoderConfigOption(func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "yaml"
	})); err != nil {
		return Config{}, fmt.Errorf("invalid config in %s: %w", resolved, err)
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func resolveConfigFilePath(p string) (string, error) {
	if filepath.IsAbs(p) {
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("config file %q: %w", p, err)
		}
		return p, nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for dir := wd; ; {
		candidate := filepath.Join(dir, p)
		if st, statErr := os.Stat(candidate); statErr == nil && !st.IsDir() {
			return candidate, nil
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return "", fmt.Errorf("config file %q: %w", candidate, statErr)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("config file %q not found (searched from %s upward)", p, wd)
		}
		dir = parent
	}
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

	// kafka (required OTLP ingest queue)
	v.SetDefault("kafka.broker_list", "")
	v.SetDefault("kafka.consumer_group", "")
	v.SetDefault("kafka.topic_prefix", "")

	// otlp
	v.SetDefault("otlp.grpc_port", "")

	// retention
	v.SetDefault("retention.default_days", 0)

	// app
	v.SetDefault("app.region", "")

	// ingestion
	v.SetDefault("ingestion.spans_bucket_seconds", 0)
	v.SetDefault("ingestion.logs_bucket_seconds", 0)
}

func (c Config) validate() error {
	if err := c.validateKafkaIngestion(); err != nil {
		return err
	}
	if !c.Redis.Enabled {
		return fmt.Errorf("redis.enabled must be true (required for sessions)")
	}
	if err := c.validateRedis(); err != nil {
		return err
	}
	isProd := strings.EqualFold(c.Environment, "production")
	if !isProd {
		return nil
	}
	var errs []string
	if c.MySQL.Password == "root123" {
		errs = append(errs, "mysql.password must be set in production")
	}
	if c.ClickHouse.Password == "clickhouse123" {
		errs = append(errs, "clickhouse.password must be set in production")
	}
	if len(errs) > 0 {
		return fmt.Errorf("insecure configuration detected: %s", strings.Join(errs, "; "))
	}
	return nil
}
