package evaluator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/dispatch"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/query"
)

// Module is the BackgroundRunner. It registers no HTTP routes (only the tick
// loop), so RegisterRoutes is a no-op. The 10s ticker drives Service.Tick.
type Module struct {
	svc      *Service
	stop     chan struct{}
	stopped  chan struct{}
	once     sync.Once
	interval time.Duration
}

func NewModule(sqlDB *registry.SQLDB, chConn clickhouse.Conn) *Module {
	repo := NewRepository(sqlDB)
	registry := query.Registry{
		Metric: query.NewMetricBackend(chConn),
		APM:    query.NewAPMBackend(chConn),
		Log:    query.NewLogBackend(chConn),
	}
	dispatcher := dispatch.NewDefaultDispatcher()
	svc := NewService(repo, registry, dispatcher)
	return &Module{
		svc:      svc,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
		interval: 10 * time.Second,
	}
}

func (m *Module) Name() string { return "alerting.evaluator" }

// RegisterRoutes is required by registry.Module; evaluator has none.
func (m *Module) RegisterRoutes(*gin.RouterGroup) {}

// Start implements registry.BackgroundRunner — spawns the tick goroutine.
func (m *Module) Start() {
	go m.run()
}

// Stop implements registry.BackgroundRunner — signals the tick goroutine to
// exit and waits for the current tick to drain.
func (m *Module) Stop() error {
	m.once.Do(func() { close(m.stop) })
	select {
	case <-m.stopped:
	case <-time.After(15 * time.Second):
		slog.Warn("alerting.evaluator: shutdown timed out")
	}
	return nil
}

func (m *Module) run() {
	defer close(m.stopped)
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), m.interval)
			if err := m.svc.Tick(ctx, now.UTC()); err != nil {
				slog.Warn("alerting.evaluator: tick failed", slog.Any("error", err))
			}
			cancel()
		}
	}
}
