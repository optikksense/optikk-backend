// Package slack hosts the Slack-facing HTTP surface: outbound test-send used
// by rule authors and the inbound callback for Slack action buttons. The
// Slack delivery channel itself lives in alerting/channels.
package slack

import (
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/gin-gonic/gin"
)

type Module struct {
	handler *Handler
}

func NewModule(handler *Handler) registry.Module {
	return &Module{handler: handler}
}

func (m *Module) Name() string                      { return "alerting.slack" }
func (m *Module) RouteTarget() registry.RouteTarget { return registry.V1 }

func (m *Module) RegisterRoutes(group *gin.RouterGroup) {
	a := group.Group("/alerts")
	a.POST("/slack/test", m.handler.TestSlack)
	a.POST("/callback/slack", m.handler.Callback)
}

var _ registry.Module = (*Module)(nil)
