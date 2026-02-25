package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/utils"
)

type tenantContextKey string

const tenantKey tenantContextKey = "tenant"

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Organization-Id, X-Team-Id, X-User-Id, X-User-Email, X-User-Role")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "Authorization, X-Organization-Id, X-Team-Id")
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

func TenantMiddleware(jwtManager auth.JWTManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if claims, err := jwtManager.Parse(token); err == nil {
				orgID := claims.OrganizationID
				if orgID == 0 {
					orgID = 1
				}
				teamID := claims.TeamID
				if teamID == 0 {
					teamID = utils.ToInt64(c.GetHeader("X-Team-Id"), 1)
				}
				role := claims.Role
				if role == "" {
					role = "member"
				}
				c.Set(string(tenantKey), types.TenantContext{
					OrganizationID: orgID,
					TeamID:         teamID,
					UserID:         utils.ToInt64(claims.Subject, 0),
					UserEmail:      claims.Email,
					UserRole:       role,
				})
				c.Next()
				return
			}
		}

		// No valid JWT — fall back to explicit X-* headers only.
		// Default role is "member", not "admin", to avoid privilege escalation.
		role := c.GetHeader("X-User-Role")
		if role == "" {
			role = "member"
		}
		c.Set(string(tenantKey), types.TenantContext{
			OrganizationID: utils.ToInt64(c.GetHeader("X-Organization-Id"), 1),
			TeamID:         utils.ToInt64(c.GetHeader("X-Team-Id"), 1),
			UserID:         utils.ToInt64(c.GetHeader("X-User-Id"), 0),
			UserEmail:      c.GetHeader("X-User-Email"),
			UserRole:       role,
		})
		c.Next()
	}
}

func GetTenant(c *gin.Context) types.TenantContext {
	v, ok := c.Get(string(tenantKey))
	if !ok {
		return types.TenantContext{OrganizationID: 1, TeamID: 1, UserID: 0, UserRole: "member"}
	}
	t, ok := v.(types.TenantContext)
	if !ok {
		return types.TenantContext{OrganizationID: 1, TeamID: 1, UserID: 0, UserRole: "member"}
	}
	return t
}
