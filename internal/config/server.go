package config

import "time"

type ServerConfig struct {
	Port           string `yaml:"port"`
	AllowedOrigins string `yaml:"allowed_origins"`
	DebugAPILogs   bool   `yaml:"debug_api_logs"`
}

type AuthConfig struct {
	JWTSecret         string `yaml:"jwt_secret"`
	AccessTTLMs       int64  `yaml:"access_ttl_ms"`
	RefreshTTLMs      int64  `yaml:"refresh_ttl_ms"`
	RefreshCookieName string `yaml:"refresh_cookie_name"`
	CookieDomain      string `yaml:"cookie_domain"`
	CookieSecure      bool   `yaml:"cookie_secure"`
	CookieSameSite    string `yaml:"cookie_same_site"`
}

type OTLPConfig struct {
	GRPCPort             string `yaml:"grpc_port"`
	GRPCMaxConcurrentStr uint32 `yaml:"grpc_max_concurrent_streams"`
}

type RetentionConfig struct {
	DefaultDays int `yaml:"default_days"`
}

type AppConfig struct {
	Region string `yaml:"region"`
}

func (c Config) AccessTokenTTL() time.Duration {
	return time.Duration(c.Auth.AccessTTLMs) * time.Millisecond
}

func (c Config) RefreshTokenTTL() time.Duration {
	return time.Duration(c.Auth.RefreshTTLMs) * time.Millisecond
}
