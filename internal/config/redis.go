package config

import (
	"fmt"
	"net"
	"strings"
	"time"
)

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

func (c Config) RedisAddr() string {
	return net.JoinHostPort(c.Redis.Host, c.Redis.Port)
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
