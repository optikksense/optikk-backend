// Package shared holds HTTP-side helpers reused across all 9 db saturation
// submodules' handlers. SQL emission lives in the sibling
// `internal/modules/saturation/database/filter/` package.
package shared

import (
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
	"github.com/gin-gonic/gin"
)

// ParseFilters extracts the standard ?db_system / ?collection / ?namespace
// /?server query-string filters into the typed shape consumed by every
// repo.
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
