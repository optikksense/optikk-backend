package registry

import (
	"database/sql"
	"errors"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

// RouteTarget indicates which router group a module's routes should be added to.
type RouteTarget int

const (
	// V1 routes are added to the uncached /api/v1 group.
	V1 RouteTarget = iota
	// Cached routes are added to the /api/v1 group with 30s Redis cache middleware.
	Cached
)

type SQLDB = sql.DB
type ClickHouseConn = clickhouse.Conn
type NativeQuerier = database.NativeQuerier
type GetTenantFunc = modulecommon.GetTenantFunc
type SessionManager = session.Manager
type AppConfig = config.Config
type TeamResolver = ingestion.TeamResolver
type SizeTracker = ingestion.SizeTracker

// LiveTailHub is the pub/sub hub interface for live-tailing.
// Defined here to avoid an import cycle (the livetail package imports registry).
type LiveTailHub interface {
	Subscribe(teamID int64, ch chan any, filter func(any) bool) bool
	Unsubscribe(teamID int64, ch chan any)
	Publish(teamID int64, event any)
}

// Deps holds all shared dependencies needed by modules.
type Deps struct {
	NativeQuerier  *NativeQuerier
	GetTenant      GetTenantFunc
	DB             *SQLDB
	CH             ClickHouseConn
	SessionManager SessionManager
	AppConfig      AppConfig
	RedisClient    *goredis.Client
	LiveTailHub    LiveTailHub
	TeamResolver   TeamResolver
	SizeTracker    SizeTracker
	KafkaBrokers   []string
	ConsumerGroup  string
	TopicPrefix    string

	closers []func() error
}

// OnClose registers a function to be called when Deps is closed.
func (d *Deps) OnClose(fn func() error) { d.closers = append(d.closers, fn) }

// Close invokes all registered closers in reverse order.
func (d *Deps) Close() error {
	var errs []error
	for i := len(d.closers) - 1; i >= 0; i-- {
		if err := d.closers[i](); err != nil {
			slog.Warn("deps close error", slog.Any("error", err))
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// NewModuleFunc is the universal module constructor signature.
type NewModuleFunc func(deps *Deps) (Module, error)

// Module is the interface every feature module implements.
type Module interface {
	Name() string
	RouteTarget() RouteTarget
	RegisterRoutes(group *gin.RouterGroup)
}

// GRPCRegistrar is implemented by modules that register gRPC services.
type GRPCRegistrar interface {
	RegisterGRPC(srv *grpc.Server)
}

// BackgroundRunner is implemented by modules that have background workers.
type BackgroundRunner interface {
	Start()
	Stop() error
}
