package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts"
	"github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type tenantContextKey string

const tenantKey tenantContextKey = "tenant"

func CORSMiddleware(allowedOrigins string) gin.HandlerFunc {
	origins := make([]string, 0, 8)
	for _, o := range strings.Split(allowedOrigins, ",") {
		origin := strings.TrimSpace(o)
		if origin == "" {
			continue
		}
		origins = append(origins, origin)
	}

	isAllowed := func(origin string) bool {
		if origin == "" {
			return false
		}
		// If no allowlist is configured, keep existing permissive behavior.
		if len(origins) == 0 {
			return true
		}
		for _, allowed := range origins {
			if allowed == "*" || allowed == origin {
				return true
			}
			if strings.HasPrefix(allowed, "*.") {
				suffix := allowed[1:]
				if strings.HasSuffix(origin, suffix) {
					return true
				}
			}
		}
		return false
	}

	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if isAllowed(origin) {
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
		slog.Error("panic recovered",
			slog.Any("error", recovered),
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.String("ip", c.ClientIP()),
			slog.String("stack", string(debug.Stack())),
		)
		c.JSON(http.StatusInternalServerError, httputil.Failure(contracts.Internal, "An unexpected error occurred", c.Request.URL.Path))
	})
}

// BodyLimitMiddleware rejects request bodies larger than maxBytes to prevent
// memory exhaustion from oversized payloads. WebSocket upgrade requests are
// excluded since they use a different framing protocol.
func BodyLimitMiddleware(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.EqualFold(c.GetHeader("Upgrade"), "websocket") {
			c.Next()
			return
		}
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		}
		c.Next()
	}
}

// publicPrefixes are paths that are always public regardless of HTTP method.
var publicPrefixes = []string{
	"/api/v1/auth/login",
	"/otlp/",
	"/health",
}

// publicPOSTPrefixes are paths that are public only for POST requests.
var publicPOSTPrefixes = []string{
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
	slog.Warn("AUTH_DENIED", slog.String("method", c.Request.Method), slog.String("path", c.Request.URL.Path), slog.String("code", "UNAUTHORIZED"), slog.String("ip", c.ClientIP()))
	c.AbortWithStatusJSON(http.StatusUnauthorized, httputil.Failure(
		contracts.Unauthorized, "Valid authentication is required", c.Request.URL.Path,
	))
}

func abortMissingTeam(c *gin.Context, email string) {
	slog.Warn("AUTH_DENIED", slog.String("method", c.Request.Method), slog.String("path", c.Request.URL.Path), slog.String("code", "MISSING_TEAM"), slog.String("user", email), slog.String("ip", c.ClientIP()))
	c.AbortWithStatusJSON(http.StatusForbidden, httputil.Failure(
		"MISSING_TEAM", "Session does not contain a valid team_id", c.Request.URL.Path,
	))
}

func abortForbiddenTeam(c *gin.Context, email string, requestedTeamID int64) {
	slog.Warn("AUTH_DENIED", slog.String("method", c.Request.Method), slog.String("path", c.Request.URL.Path), slog.String("code", "FORBIDDEN_TEAM"), slog.String("user", email), slog.Int64("requested_team", requestedTeamID), slog.String("ip", c.ClientIP()))
	c.AbortWithStatusJSON(http.StatusForbidden, httputil.Failure(
		"FORBIDDEN_TEAM", "You are not a member of the requested team", c.Request.URL.Path,
	))
}

// resolveTeam returns the effective team ID for the request.
// It aborts c and returns (0, false) on any auth violation.
func resolveTeam(c *gin.Context, state session.AuthState) (int64, bool) {
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

func TenantMiddleware(sessions session.Manager) gin.HandlerFunc {
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

		c.Set(string(tenantKey), contracts.TenantContext{
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

// RequireRole returns middleware that restricts access to users whose role
// matches one of the allowed roles. Must be placed after TenantMiddleware.
func RequireRole(allowed ...string) gin.HandlerFunc {
	roleSet := make(map[string]struct{}, len(allowed))
	for _, r := range allowed {
		roleSet[r] = struct{}{}
	}
	return func(c *gin.Context) {
		tenant := GetTenant(c)
		if _, ok := roleSet[tenant.UserRole]; !ok {
			slog.Warn("RBAC_DENIED",
				slog.String("method", c.Request.Method),
				slog.String("path", c.Request.URL.Path),
				slog.String("role", tenant.UserRole),
				slog.String("user", tenant.UserEmail),
				slog.String("ip", c.ClientIP()),
			)
			c.AbortWithStatusJSON(http.StatusForbidden, httputil.Failure(
				"FORBIDDEN_ROLE", "Insufficient permissions", c.Request.URL.Path,
			))
			return
		}
		c.Next()
	}
}

func GetTenant(c *gin.Context) contracts.TenantContext {
	v, ok := c.Get(string(tenantKey))
	if !ok {
		return contracts.TenantContext{}
	}
	t, ok := v.(contracts.TenantContext)
	if !ok {
		return contracts.TenantContext{}
	}
	return t
}
