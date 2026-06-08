package registry

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

type SQLDB = sql.DB
type ClickHouseConn = clickhouse.Conn
type GetTenantFunc = modulecommon.GetTenantFunc
type AppConfig = config.Config

// AuthState holds user information stored in the session.
type AuthState struct {
	UserID        int64
	Email         string
	Role          string
	DefaultTeamID int64
	TeamIDs       []int64
}

// SessionManager defines the interface for managing sessions.
type SessionManager interface {
	Wrap(next http.Handler) http.Handler
	CreateAuthSession(ctx context.Context, state AuthState) error
	DestroySession(ctx context.Context) error
	GetAuthState(ctx context.Context) (AuthState, bool)
}

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
