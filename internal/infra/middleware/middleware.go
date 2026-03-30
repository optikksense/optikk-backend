package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	sessionauth "github.com/Optikk-Org/optikk-backend/internal/infra/session"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	types "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const RequestIDKey = "requestId"

func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-Id")
		if id == "" {
			b := make([]byte, 16)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
		}
		c.Set(RequestIDKey, id)
		c.Writer.Header().Set("X-Request-Id", id)
		c.Next()
	}
}

func GetRequestID(c *gin.Context) string {
	if id, ok := c.Get(RequestIDKey); ok {
		s, _ := id.(string)
		return s
	}
	return ""
}

func APIDebugLogger(enabled bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !enabled {
			c.Next()
			return
		}

		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		c.Next()

		timestamp := time.Now()
		latency := timestamp.Sub(start)
		method := c.Request.Method
		statusCode := c.Writer.Status()

		if raw != "" {
			path = path + "?" + raw
		}

		logger.L().Debug("API request", zap.String("method", method), zap.String("path", path), zap.Int("status", statusCode), zap.Duration("latency", latency))
	}
}

type tenantContextKey string

const tenantKey tenantContextKey = "tenant"

func CORSMiddleware(allowedOrigins string) gin.HandlerFunc {
	originSet := make(map[string]bool)
	for _, o := range strings.Split(allowedOrigins, ",") {
		origin := strings.TrimSpace(o)
		if origin == "" {
			continue
		}
		originSet[origin] = true
	}

	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if originSet[origin] {
			c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
			c.Writer.Header().Set("Vary", "Origin")
		}
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Team-Id, X-User-Id, X-User-Email, X-User-Role")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "X-Team-Id")
		// Allow cookies to be sent cross-origin (required for httpOnly cookie auth).
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

func ErrorRecovery() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		c.JSON(http.StatusInternalServerError, types.Failure(errorcode.Internal, "An unexpected error occurred", c.Request.URL.Path))
	})
}

// publicPrefixes are paths that are always public regardless of HTTP method.
var publicPrefixes = []string{
	"/api/v1/auth/login",
	"/api/v1/auth/google",
	"/api/v1/auth/github",
	"/otlp/",
	"/health",
}

// publicPOSTPrefixes are paths that are public only for POST requests.
var publicPOSTPrefixes = []string{
	"/api/v1/auth/oauth/complete-signup",
	"/api/v1/auth/forgot-password",
	"/api/v1/users",
	"/api/v1/teams",
}

// isPublicRequest returns true if the request method and path do not require authentication.
func isPublicRequest(method, path string) bool {
	for _, p := range publicPrefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	if method == http.MethodPost {
		for _, p := range publicPOSTPrefixes {
			if strings.HasPrefix(path, p) {
				return true
			}
		}
	}
	return false
}

func abortUnauthorized(c *gin.Context) {
	logger.L().Warn("AUTH_DENIED", zap.String("method", c.Request.Method), zap.String("path", c.Request.URL.Path), zap.String("code", "UNAUTHORIZED"), zap.String("ip", c.ClientIP()))
	c.AbortWithStatusJSON(http.StatusUnauthorized, types.Failure(
		errorcode.Unauthorized, "Valid authentication is required", c.Request.URL.Path,
	))
}

func abortMissingTeam(c *gin.Context, email string) {
	logger.L().Warn("AUTH_DENIED", zap.String("method", c.Request.Method), zap.String("path", c.Request.URL.Path), zap.String("code", "MISSING_TEAM"), zap.String("user", email), zap.String("ip", c.ClientIP()))
	c.AbortWithStatusJSON(http.StatusForbidden, types.Failure(
		"MISSING_TEAM", "Session does not contain a valid team_id", c.Request.URL.Path,
	))
}

func abortForbiddenTeam(c *gin.Context, email string, requestedTeamID int64) {
	logger.L().Warn("AUTH_DENIED", zap.String("method", c.Request.Method), zap.String("path", c.Request.URL.Path), zap.String("code", "FORBIDDEN_TEAM"), zap.String("user", email), zap.Int64("requested_team", requestedTeamID), zap.String("ip", c.ClientIP()))
	c.AbortWithStatusJSON(http.StatusForbidden, types.Failure(
		"FORBIDDEN_TEAM", "You are not a member of the requested team", c.Request.URL.Path,
	))
}

// resolveTeam returns the effective team ID for the request.
// It aborts c and returns (0, false) on any auth violation.
func resolveTeam(c *gin.Context, state sessionauth.AuthState) (int64, bool) {
	requested := utils.ToInt64(c.GetHeader("X-Team-Id"), 0)
	if requested == 0 {
		if state.DefaultTeamID == 0 {
			abortMissingTeam(c, state.Email)
			return 0, false
		}
		return state.DefaultTeamID, true
	}
	if !authorizedForTeam(state.TeamIDs, state.DefaultTeamID, requested) {
		abortForbiddenTeam(c, state.Email, requested)
		return 0, false
	}
	return requested, true
}

func TenantMiddleware(sessions *sessionauth.Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		authState, ok := sessions.GetAuthState(c.Request.Context())
		if !ok {
			if isPublicRequest(c.Request.Method, c.Request.URL.Path) {
				c.Next()
				return
			}
			abortUnauthorized(c)
			return
		}

		teamID, ok := resolveTeam(c, authState)
		if !ok {
			return
		}

		role := authState.Role
		if role == "" {
			role = "member"
		}

		c.Set(string(tenantKey), types.TenantContext{
			TeamID:    teamID,
			UserID:    authState.UserID,
			UserEmail: authState.Email,
			UserRole:  role,
		})
		c.Next()
	}
}

func authorizedForTeam(teamIDs []int64, defaultTeamID, requestedTeamID int64) bool {
	if len(teamIDs) == 0 {
		return defaultTeamID == requestedTeamID
	}
	for _, teamID := range teamIDs {
		if teamID == requestedTeamID {
			return true
		}
	}
	return false
}

func GetTenant(c *gin.Context) types.TenantContext {
	v, ok := c.Get(string(tenantKey))
	if !ok {
		return types.TenantContext{}
	}
	t, ok := v.(types.TenantContext)
	if !ok {
		return types.TenantContext{}
	}
	return t
}
