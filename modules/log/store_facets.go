package logs

import (
	"context"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// GetLogFacets returns total count and facet breakdowns for level, service, host.
// This is a dedicated endpoint, decoupled from paginated search.
func (r *ClickHouseRepository) GetLogFacets(ctx context.Context, f LogFilters) (LogFacetsResponse, error) {
	where, args := buildLogWhere(f)
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+where, args...)

	levelRows, levelErr := dbutil.QueryMaps(r.db, `SELECT level as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY level ORDER BY count DESC`, args...)
	serviceRows, svcErr := dbutil.QueryMaps(r.db, `SELECT service_name as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY service_name ORDER BY count DESC LIMIT 50`, args...)
	hostRows, hostErr := dbutil.QueryMaps(r.db, `SELECT host as value, COUNT(*) as count FROM logs WHERE`+where+` AND host != '' GROUP BY host ORDER BY count DESC LIMIT 50`, args...)

	if levelErr != nil || svcErr != nil || hostErr != nil {
		fmt.Printf("logs: facet query errors: level=%v service=%v host=%v\n", levelErr, svcErr, hostErr)
	}

	return LogFacetsResponse{
		Total: total,
		Facets: map[string][]Facet{
			"levels":   mapRowsToFacets(levelRows),
			"services": mapRowsToFacets(serviceRows),
			"hosts":    mapRowsToFacets(hostRows),
		},
	}, nil
}

// GetLogStats returns total count and extended facet breakdowns.
func (r *ClickHouseRepository) GetLogStats(ctx context.Context, f LogFilters) (LogStats, error) {
	where, args := buildLogWhere(f)
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+where, args...)

	levelRows, levelErr := dbutil.QueryMaps(r.db, `SELECT level as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY level ORDER BY count DESC`, args...)
	serviceRows, svcErr := dbutil.QueryMaps(r.db, `SELECT service_name as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY service_name ORDER BY count DESC LIMIT 50`, args...)
	hostRows, hostErr := dbutil.QueryMaps(r.db, `SELECT host as value, COUNT(*) as count FROM logs WHERE`+where+` AND host != '' GROUP BY host ORDER BY count DESC LIMIT 50`, args...)
	podRows, podErr := dbutil.QueryMaps(r.db, `SELECT pod as value, COUNT(*) as count FROM logs WHERE`+where+` AND pod != '' GROUP BY pod ORDER BY count DESC LIMIT 50`, args...)
	loggerRows, loggerErr := dbutil.QueryMaps(r.db, `SELECT logger as value, COUNT(*) as count FROM logs WHERE`+where+` AND logger != '' GROUP BY logger ORDER BY count DESC LIMIT 50`, args...)

	if levelErr != nil || svcErr != nil || hostErr != nil || podErr != nil || loggerErr != nil {
		fmt.Printf("logs: stats facet query errors: level=%v service=%v host=%v pod=%v logger=%v\n",
			levelErr, svcErr, hostErr, podErr, loggerErr)
	}

	return LogStats{
		Total: total,
		Fields: map[string][]Facet{
			"level":        mapRowsToFacets(levelRows),
			"service_name": mapRowsToFacets(serviceRows),
			"host":         mapRowsToFacets(hostRows),
			"pod":          mapRowsToFacets(podRows),
			"logger":       mapRowsToFacets(loggerRows),
		},
	}, nil
}

// GetLogFields returns facet values for a single column.
func (r *ClickHouseRepository) GetLogFields(ctx context.Context, f LogFilters, col string) ([]Facet, error) {
	where, args := buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as value, COUNT(*) as count
		FROM logs WHERE%s AND %s != ''
		GROUP BY %s ORDER BY count DESC LIMIT 200`, col, where, col, col)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	return mapRowsToFacets(rows), nil
}
