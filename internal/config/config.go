package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
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

type QueueConfig struct {
	BatchSize       int   `yaml:"batch_size"`
	FlushIntervalMs int64 `yaml:"flush_interval_ms"`
	Capacity        int   `yaml:"capacity"`
}

type KafkaConfig struct {
	Enabled bool   `yaml:"enabled"`
	Brokers string `yaml:"brokers"`
}

type RedisConfig struct {
	Enabled bool   `yaml:"enabled"`
	Host    string `yaml:"host"`
	Port    string `yaml:"port"`
}

type OTLPConfig struct {
	GRPCPort             string `yaml:"grpc_port"`
	GRPCMaxRecvMsgSizeMB int    `yaml:"grpc_max_recv_msg_size_mb"`
}

type OAuthConfig struct {
	GoogleClientID     string `yaml:"google_client_id"`
	GoogleClientSecret string `yaml:"google_client_secret"`
	GitHubClientID     string `yaml:"github_client_id"`
	GitHubClientSecret string `yaml:"github_client_secret"`
	RedirectBase       string `yaml:"redirect_base"`
}

type RetentionConfig struct {
	DefaultDays int `yaml:"default_days"`
}

type AppConfig struct {
	Region                     string `yaml:"region"`
	DashboardConfigUseDefaults bool   `yaml:"dashboard_config_use_defaults"`
}

type Config struct {
	Environment string           `yaml:"environment"`
	Server      ServerConfig     `yaml:"server"`
	MySQL       MySQLConfig      `yaml:"mysql"`
	ClickHouse  ClickHouseConfig `yaml:"clickhouse"`
	Session     SessionConfig    `yaml:"session"`
	Queue       QueueConfig      `yaml:"queue"`
	Kafka       KafkaConfig      `yaml:"kafka"`
	Redis       RedisConfig      `yaml:"redis"`
	OTLP        OTLPConfig       `yaml:"otlp"`
	OAuth       OAuthConfig      `yaml:"oauth"`
	Retention   RetentionConfig  `yaml:"retention"`
	App         AppConfig        `yaml:"app"`
}

// Load reads configuration from a YAML file.
// If no path is provided, it defaults to "config.yml".
// In production (environment: production), passwords must not use insecure defaults.
func Load(path ...string) Config {
	p := "config.yml"
	if len(path) > 0 && path[0] != "" {
		p = path[0]
	}

	data, err := os.ReadFile(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: cannot read config file %s: %v\n", p, err)
		os.Exit(1)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: invalid config YAML in %s: %v\n", p, err)
		os.Exit(1)
	}

	cfg.validate()
	return cfg
}

func (c Config) validate() {
	isProd := strings.EqualFold(c.Environment, "production")
	if !isProd {
		return
	}
	var errs []string
	if c.MySQL.Password == "root123" {
		errs = append(errs, "mysql.password must be set in production (do not use the default)")
	}
	if c.ClickHouse.Password == "clickhouse123" {
		errs = append(errs, "clickhouse.password must be set in production (do not use the default)")
	}
	if c.ClickHouse.Production && c.ClickHouse.CloudHost == "" {
		errs = append(errs, "clickhouse.cloud_host must be set when clickhouse.production is true")
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

func (c Config) SessionLifetime() time.Duration {
	return time.Duration(c.Session.LifetimeMs) * time.Millisecond
}

func (c Config) SessionIdleTimeout() time.Duration {
	return time.Duration(c.Session.IdleTimeoutMs) * time.Millisecond
}

func (c Config) QueueFlushInterval() time.Duration {
	return time.Duration(c.Queue.FlushIntervalMs) * time.Millisecond
}

func (c Config) KafkaBrokerList() []string {
	return strings.Split(c.Kafka.Brokers, ",")
}
