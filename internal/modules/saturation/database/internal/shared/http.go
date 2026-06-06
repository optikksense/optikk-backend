// Package shared holds HTTP-side helpers reused across database saturation
// submodules' handlers.
package shared

import (
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
	"github.com/gin-gonic/gin"
)

// ParseFilters extracts query-string filters into the typed shape consumed
// by every repository.
func ParseFilters(c *gin.Context) filter.Filters {
	return filter.Filters{
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
