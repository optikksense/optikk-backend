package api

import (
	"strings"

	"github.com/gin-gonic/gin"
	util "github.com/observability/observability-backend-go/internal/helpers"
)

// resolveAPIKey extracts the api_key from Authorization: Bearer <key> or
// X-API-Key: <key> and returns the team UUID if the key is valid.
func (h *Handler) resolveAPIKey(c *gin.Context) (string, bool) {
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
	if err := h.MySQL.QueryRow(
		`SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1`, apiKey,
	).Scan(&teamID); err != nil {
		return "", false
	}
	return util.ToTeamUUID(teamID), true
}
