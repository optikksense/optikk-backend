package shared

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

func ParseFilters(c *gin.Context) Filters {
	return Filters{
		DBSystem:   c.QueryArray("db_system"),
		Collection: c.QueryArray("collection"),
		Namespace:  c.QueryArray("namespace"),
		Server:     c.QueryArray("server"),
	}
}

func ParseLimit(c *gin.Context, def int) int {
	if s := c.Query("limit"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			return v
		}
	}
	return def
}

func ParseThreshold(c *gin.Context, def float64) float64 {
	if s := c.Query("threshold_ms"); s != "" {
		if v, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err == nil && v > 0 {
			return v
		}
	}
	return def
}

func RequireCollection(c *gin.Context) (string, bool) {
	v := c.Query("collection")
	if v == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, "MISSING_PARAM", "collection query param is required")
		return "", false
	}
	return v, true
}

func RequireDBSystem(c *gin.Context) (string, bool) {
	v := c.Query("db_system")
	if v == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, "MISSING_PARAM", "db_system query param is required")
		return "", false
	}
	return v, true
}
