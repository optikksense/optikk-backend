package config

import "time"

type ServerConfig struct {
	Port           string `yaml:"port"`
	AllowedOrigins string `yaml:"allowed_origins"`
	DebugAPILogs   bool   `yaml:"debug_api_logs"`
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

type OTLPConfig struct {
	GRPCPort string `yaml:"grpc_port"`
}

type RetentionConfig struct {
	DefaultDays int `yaml:"default_days"`
}

type AppConfig struct {
	Region string `yaml:"region"`
}

func (c Config) SessionLifetime() time.Duration {
	return time.Duration(c.Session.LifetimeMs) * time.Millisecond
}

func (c Config) SessionIdleTimeout() time.Duration {
	return time.Duration(c.Session.IdleTimeoutMs) * time.Millisecond
}
