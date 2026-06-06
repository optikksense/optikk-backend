package config

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// OtlRedisStream configures Redis Streams OTLP ingest.
type OtlRedisStream struct {
	// MaxLenApprox is the approximate MAXLEN per ingest stream.
	MaxLenApprox int64 `yaml:"max_len_approx"`
	// LogsMaxLenApprox is the approximate MAXLEN for the logs ingest stream.
	LogsMaxLenApprox int64 `yaml:"logs_max_len_approx"`
	// StreamTTLSeconds is the maximum age of stream entries for pruning.
	StreamTTLSeconds int64 `yaml:"stream_ttl_seconds"`
	// ChBatchSize is max rows per ClickHouse flush from stream reads.
	ChBatchSize int `yaml:"ch_batch_size"`
	// ChFlushIntervalMs bounds ClickHouse flush wait for partial batches.
	ChFlushIntervalMs int64 `yaml:"ch_flush_interval_ms"`
	// XReadBlockMs is the BLOCK timeout for XREADGROUP.
	XReadBlockMs int64 `yaml:"xread_block_ms"`
	// XReadCount is COUNT for each XREADGROUP batch.
	XReadCount int64 `yaml:"xread_count"`
}

type RedisConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Password string `yaml:"password"`
	// DB selects the Redis logical database index (default 0).
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
		// default for logs
		return 5000
	}
	return c.OtlRedisStream.MaxLenApprox
}

func (c Config) OtlSpansMaxLen() int64 {
	if c.OtlRedisStream.MaxLenApprox <= 0 {
		// default for spans
		return 1000
	}
	return c.OtlRedisStream.MaxLenApprox
}

func (c Config) OtlStreamTTL() time.Duration {
	n := c.OtlRedisStream.StreamTTLSeconds
	if n <= 0 {
		return 3600 * time.Second
	}
	return time.Duration(n) * time.Second
}
