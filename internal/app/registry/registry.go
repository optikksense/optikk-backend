package registry

import (
	"database/sql"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

type SQLDB = sql.DB
type ClickHouseConn = clickhouse.Conn
type GetTenantFunc = modulecommon.GetTenantFunc
type SessionManager = session.Manager
type AppConfig = config.Config

// Module is the interface every feature module implements.
type Module interface {
	Name() string
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
