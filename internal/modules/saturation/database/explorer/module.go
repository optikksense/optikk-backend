package explorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	dbconnections "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/connections"
	dberrors "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/errors"
	dblatency "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/latency"
	dbslowqueries "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/slowqueries"
	dbsummary "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/summary"
	dbsystem "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/system"
	dbsystems "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/systems"
	dbvolume "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/volume"
	saturationkafka "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka"
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

	v1.GET("/saturation/kafka/summary", h.GetKafkaSummary)
	v1.GET("/saturation/kafka/topics", h.GetKafkaTopics)
	v1.GET("/saturation/kafka/groups", h.GetKafkaGroups)
	v1.GET("/saturation/kafka/topic/overview", h.GetKafkaTopicOverview)
	v1.GET("/saturation/kafka/topic/groups", h.GetKafkaTopicGroups)
	v1.GET("/saturation/kafka/topic/partitions", h.GetKafkaTopicPartitions)
	v1.GET("/saturation/kafka/group/overview", h.GetKafkaGroupOverview)
	v1.GET("/saturation/kafka/group/topics", h.GetKafkaGroupTopics)
	v1.GET("/saturation/kafka/group/partitions", h.GetKafkaGroupPartitions)
}

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	module := &saturationExplorerModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type saturationExplorerModule struct {
	handler *Handler
}

func (m *saturationExplorerModule) Name() string                      { return "saturationExplorer" }
func (m *saturationExplorerModule) RouteTarget() registry.RouteTarget { return registry.Cached }

func (m *saturationExplorerModule) configure(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) {
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
			saturationkafka.NewService(saturationkafka.NewRepository(nativeQuerier)),
		),
	}
}

func (m *saturationExplorerModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler)
}
