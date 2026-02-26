package api

import (
	"database/sql"
	"strings"

	"github.com/gin-gonic/gin"
	util "github.com/observability/observability-backend-go/internal/helpers"
)

// APIKeyResolver extracts and validates an API key from an HTTP request.
type APIKeyResolver interface {
	ResolveAPIKey(c *gin.Context) (teamUUID string, ok bool)
}

// mysqlAPIKeyResolver validates API keys against the teams table in MySQL.
type mysqlAPIKeyResolver struct {
	db *sql.DB
}

func newMySQLAPIKeyResolver(db *sql.DB) APIKeyResolver {
	return &mysqlAPIKeyResolver{db: db}
}

func (r *mysqlAPIKeyResolver) ResolveAPIKey(c *gin.Context) (string, bool) {
	apiKey := ""
	if auth := c.GetHeader("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		apiKey = strings.TrimPrefix(auth, "Bearer ")
	}
	if apiKey == "" {
		apiKey = c.GetHeader("X-API-Key")
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return "", false
	}

	var teamID int64
	if err := r.db.QueryRow(
		`SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1`, apiKey,
	).Scan(&teamID); err != nil {
		return "", false
	}
	return util.ToTeamUUID(teamID), true
}
