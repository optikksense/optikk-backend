package middleware

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/utils"
)

type tenantContextKey string

const tenantKey tenantContextKey = "tenant"

// CORSMiddleware restricts cross-origin requests to explicitly allowed origins.
// Set ALLOWED_ORIGINS env var to a comma-separated list of origins (e.g. "https://app.example.com,https://dashboard.example.com").
// Falls back to "http://localhost:5173" only in development when ALLOWED_ORIGINS is not set.
func CORSMiddleware() gin.HandlerFunc {
	allowedOrigins := os.Getenv("ALLOWED_ORIGINS")
	originSet := make(map[string]bool)
	if allowedOrigins != "" {
		for _, o := range strings.Split(allowedOrigins, ",") {
			originSet[strings.TrimSpace(o)] = true
		}
	}

	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if len(originSet) > 0 {
			if originSet[origin] {
				c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
				c.Writer.Header().Set("Vary", "Origin")
			}
			// If origin is not in the allowed set, omit the header entirely.
		} else {
			// No ALLOWED_ORIGINS configured — default to strict local development mode.
			// Prevent wildcard '*' in production if forgot to set env var.
			c.Writer.Header().Set("Access-Control-Allow-Origin", "http://localhost:5173")
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

// TenantMiddleware extracts the authenticated tenant from the JWT token.
// Public paths (signup, login, OTLP ingestion) are allowed through without
// authentication. All other paths require a valid JWT; requests without one
// are rejected with 401.
//
// When a non-nil TokenBlacklist is provided, revoked tokens are rejected
// with 401 even if they are otherwise valid.
//
// Fix 1: Accepts JWT from httpOnly cookie as fallback to Authorization header.
// Fix 2: Validates X-Team-Id override against the JWT's Teams claim to
//
//	prevent cross-tenant data access via forged headers.
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
				c.AbortWithStatusJSON(http.StatusUnauthorized, types.Failure(
					"TOKEN_REVOKED", "Token has been revoked", c.Request.URL.Path,
				))
				return
			}

			if claims, err := jwtManager.Parse(token); err == nil {
				teamID := claims.TeamID

				// Allow explicit team switching from UI for users that belong to
				// multiple teams. JWT claim remains the default team.
				// Fix 2: validate the requested team against the claims before allowing override.
				if requested := utils.ToInt64(c.GetHeader("X-Team-Id"), 0); requested > 0 {
					if !auth.ClaimsAuthorizedForTeam(claims, requested) {
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
		c.AbortWithStatusJSON(http.StatusUnauthorized, types.Failure(
			"UNAUTHORIZED", "Valid authentication is required", c.Request.URL.Path,
		))
	}
}

// GetTenant extracts the TenantContext set by TenantMiddleware.
// Returns a zero-value context if no tenant was set (e.g. on public routes).
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
