package explorer

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbconnections "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/connections"
	dberrors "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/errors"
	dblatency "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/latency"
	dbslowqueries "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/slowqueries"
	dbsummary "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/summary"
	dbsystem "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/system"
	dbsystems "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/systems"
	dbvolume "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/volume"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/saturation/datastores/summary", h.GetDatastoreSummary)
	v1.GET("/saturation/datastores/systems", h.GetDatastoreSystems)
	v1.GET("/saturation/datastores/system/overview", h.GetDatastoreSystemOverview)
	v1.GET("/saturation/datastores/system/servers", h.GetDatastoreSystemServers)
	v1.GET("/saturation/datastores/system/namespaces", h.GetDatastoreSystemNamespaces)
	v1.GET("/saturation/datastores/system/operations", h.GetDatastoreSystemOperations)
	v1.GET("/saturation/datastores/system/errors", h.GetDatastoreSystemErrors)
	v1.GET("/saturation/datastores/system/connections", h.GetDatastoreSystemConnections)
	v1.GET("/saturation/datastores/system/slow-queries", h.GetDatastoreSystemSlowQueries)
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &saturationExplorerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type saturationExplorerModule struct {
	handler *Handler
}

func (m *saturationExplorerModule) Name() string { return "saturationDatastoresExplorer" }

func (m *saturationExplorerModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	m.handler = &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service: NewService(
			dbsummary.NewService(dbsummary.NewRepository(nativeQuerier)),
			dbsystems.NewService(dbsystems.NewRepository(nativeQuerier)),
			dbsystem.NewService(dbsystem.NewRepository(nativeQuerier)),
			dblatency.NewService(dblatency.NewRepository(nativeQuerier)),
			dbvolume.NewService(dbvolume.NewRepository(nativeQuerier)),
			dberrors.NewService(dberrors.NewRepository(nativeQuerier)),
			dbslowqueries.NewService(dbslowqueries.NewRepository(nativeQuerier)),
			dbconnections.NewService(dbconnections.NewRepository(nativeQuerier)),
		),
	}
}

func (m *saturationExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
