package redis

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	goredis "github.com/redis/go-redis/v9"
)

// redis metrics live here rather than in internal/infra/metrics because
// they're specific to this subsystem and the collector is a local-import
// go-redis hook. Keeps the metrics package free of go-redis dependency.
var (
	redisCommandsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "optikk",
		Subsystem: "redis",
		Name:      "commands_total",
		Help:      "Redis commands executed, by command name + result (ok/err).",
	}, []string{"cmd", "result"})

	redisCommandDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "optikk",
		Subsystem: "redis",
		Name:      "command_duration_seconds",
		Help:      "Redis command latency in seconds, by command name.",
		Buckets:   []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5},
	}, []string{"cmd"})
)

type metricsHook struct{}

var _ goredis.Hook = metricsHook{}

// DialHook is a pass-through — connect-level timing doesn't add enough
// signal over the existing go-redis pool metrics.
func (metricsHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (metricsHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		observeCmd(cmd.Name(), start, err)
		return err
	}
}

func (metricsHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		// Collapse pipeline under `pipeline` so cardinality stays bounded.
		observeCmd("pipeline", start, err)
		return err
	}
}

func observeCmd(name string, start time.Time, err error) {
	cmd := strings.ToLower(name)
	if cmd == "" {
		cmd = "unknown"
	}
	result := "ok"
	if err != nil {
		result = "err"
	}
	redisCommandsTotal.WithLabelValues(cmd, result).Inc()
	redisCommandDuration.WithLabelValues(cmd).Observe(time.Since(start).Seconds())
}
