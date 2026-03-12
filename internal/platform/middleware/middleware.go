package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/utils"
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
		return id.(string)
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

		log.Printf("[DEBUG API] %s %s | %d | %v", method, path, statusCode, latency)
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
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Team-Id, X-User-Id, X-User-Email, X-User-Role")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "Authorization, X-Team-Id")
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
		c.JSON(http.StatusInternalServerError, types.Failure("INTERNAL_ERROR", "An unexpected error occurred", c.Request.URL.Path))
	})
}

// isPublicRequest returns true if the request method and path do not require JWT authentication.
func isPublicRequest(method, path string) bool {
	// auth routes need to be accessible without a token.
	if strings.HasPrefix(path, "/api/v1/auth/login") {
		return true
	}

	// OAuth provider redirects and callbacks.
	if strings.HasPrefix(path, "/api/v1/auth/google") {
		return true
	}
	if strings.HasPrefix(path, "/api/v1/auth/github") {
		return true
	}

	// OAuth complete-signup and forgot-password are public POST endpoints.
	if method == http.MethodPost && strings.HasPrefix(path, "/api/v1/auth/oauth/complete-signup") {
		return true
	}
	if method == http.MethodPost && strings.HasPrefix(path, "/api/v1/auth/forgot-password") {
		return true
	}

	// signup/create user must be public to allow first login.
	if method == http.MethodPost && (path == "/api/v1/users" || path == "/api/v1/users/") {
		return true
	}

	// create team must be public.
	if method == http.MethodPost && (path == "/api/v1/teams" || path == "/api/v1/teams/") {
		return true
	}

	// OTLP endpoints use API-key auth in their own handler.
	if strings.HasPrefix(path, "/otlp/") {
		return true
	}

	// Health check endpoints are always public.
	if strings.HasPrefix(path, "/health") {
		return true
	}

	return false
}

// extractBearerToken extracts a JWT token string from the request.
// Priority: Authorization header → httpOnly cookie "token".
// This allows browser clients to use the secure cookie flow while
// SDK/CLI clients continue using the Authorization header.
func extractBearerToken(c *gin.Context) string {
	if authHeader := c.GetHeader("Authorization"); strings.HasPrefix(authHeader, "Bearer ") {
		return strings.TrimPrefix(authHeader, "Bearer ")
	}
	// Fall back to the httpOnly cookie set by the login handler.
	if cookie, err := c.Cookie("token"); err == nil && cookie != "" {
		return cookie
	}
	return ""
}

// TenantMiddleware accepts JWTs from the Authorization header or auth cookie.
// Public routes pass through unauthenticated, but any X-Team-Id override must
// be authorized by the token claims.
func TenantMiddleware(jwtManager auth.JWTManager, blacklist ...*auth.TokenBlacklist) gin.HandlerFunc {
	var bl *auth.TokenBlacklist
	if len(blacklist) > 0 {
		bl = blacklist[0]
	}

	return func(c *gin.Context) {
		token := extractBearerToken(c)

		if token != "" {
			// Reject revoked tokens before spending time on signature verification.
			if bl != nil && bl.IsRevoked(token) {
				log.Printf("AUTH_DENIED [%s %s] code=TOKEN_REVOKED ip=%s", c.Request.Method, c.Request.URL.Path, c.ClientIP())
				c.AbortWithStatusJSON(http.StatusUnauthorized, types.Failure(
					"TOKEN_REVOKED", "Token has been revoked", c.Request.URL.Path,
				))
				return
			}

			if claims, err := jwtManager.Parse(token); err == nil {
				teamID := claims.TeamID

				// Allow explicit team switching from UI for users that belong to
				// multiple teams. JWT claim remains the default team.
				if requested := utils.ToInt64(c.GetHeader("X-Team-Id"), 0); requested > 0 {
					if !auth.ClaimsAuthorizedForTeam(claims, requested) {
						log.Printf("AUTH_DENIED [%s %s] code=FORBIDDEN_TEAM user=%s requested_team=%d ip=%s",
							c.Request.Method, c.Request.URL.Path, claims.Email, requested, c.ClientIP())
						c.AbortWithStatusJSON(http.StatusForbidden, types.Failure(
							"FORBIDDEN_TEAM",
							"You are not a member of the requested team",
							c.Request.URL.Path,
						))
						return
					}
					teamID = requested
				}

				if teamID == 0 {
					log.Printf("AUTH_DENIED [%s %s] code=MISSING_TEAM user=%s ip=%s",
						c.Request.Method, c.Request.URL.Path, claims.Email, c.ClientIP())
					c.AbortWithStatusJSON(http.StatusForbidden, types.Failure(
						"MISSING_TEAM", "JWT does not contain a valid team_id", c.Request.URL.Path,
					))
					return
				}
				role := claims.Role
				if role == "" {
					role = "member"
				}
				c.Set(string(tenantKey), types.TenantContext{
					TeamID:    teamID,
					UserID:    utils.ToInt64(claims.Subject, 0),
					UserEmail: claims.Email,
					UserRole:  role,
				})
				c.Next()
				return
			}
		}

		// No valid JWT — allow public paths through without authentication.
		if isPublicRequest(c.Request.Method, c.Request.URL.Path) {
			c.Next()
			return
		}

		// Protected path without valid JWT — reject.
		log.Printf("AUTH_DENIED [%s %s] code=UNAUTHORIZED ip=%s", c.Request.Method, c.Request.URL.Path, c.ClientIP())
		c.AbortWithStatusJSON(http.StatusUnauthorized, types.Failure(
			"UNAUTHORIZED", "Valid authentication is required", c.Request.URL.Path,
		))
	}
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
