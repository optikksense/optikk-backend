package otel

import (
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

// Middleware returns the otelgin middleware bound to the backend service
// name. Kept as a thin re-export so routes.go doesn't need to know about
// otelgin directly — swap implementations here if the middleware changes.
func Middleware() gin.HandlerFunc {
	return otelgin.Middleware("optikk-backend")
}
