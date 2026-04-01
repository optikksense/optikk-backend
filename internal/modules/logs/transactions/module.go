package transactions

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

func NewModule(nativeQuerier *registry.NativeQuerier, getTenant registry.GetTenantFunc) registry.Module {
	return &logTransactionsModule{
		handler: NewHandler(getTenant, NewService(nativeQuerier)),
	}
}

type logTransactionsModule struct {
	handler *Handler
}

func (m *logTransactionsModule) Name() string                      { return "logTransactions" }
func (m *logTransactionsModule) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *logTransactionsModule) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/logs/transactions", m.handler.GetTransactions)
}
