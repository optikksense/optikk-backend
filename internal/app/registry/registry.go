package registry

import (
	"database/sql"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	configdefaults "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	sessionauth "github.com/Optikk-Org/optikk-backend/internal/infra/session"
	sio "github.com/Optikk-Org/optikk-backend/internal/infra/socketio"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
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
type SessionManager = sessionauth.Manager
type AppConfig = config.Config
type ConfigRegistry = configdefaults.Registry

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

// SocketIORegistrar is implemented by modules that register Socket.IO handlers.
type SocketIORegistrar interface {
	RegisterSocketIO(srv *sio.Server)
}

