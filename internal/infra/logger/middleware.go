package logger

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const RequestIDHeader = "X-Request-ID"

// GinMiddleware returns Gin middleware that:
//  1. Assigns a request ID (from header or generated)
//  2. Attaches a child logger with request fields to the request context
//  3. Logs the completed request with status, latency, and method/path
func GinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		requestID := c.GetHeader(RequestIDHeader)
		if requestID == "" {
			requestID = uuid.New().String()
		}
		c.Header(RequestIDHeader, requestID)

		reqLogger := L().With(
			zap.String("request_id", requestID),
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
		)
		c.Request = c.Request.WithContext(WithCtx(c.Request.Context(), reqLogger))

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		fields := []zap.Field{
			zap.Int("status", status),
			zap.Duration("latency", latency),
		}

		switch {
		case status >= 500:
			reqLogger.Error("request completed", fields...)
		case status >= 400:
			reqLogger.Warn("request completed", fields...)
		default:
			reqLogger.Info("request completed", fields...)
		}
	}
}
