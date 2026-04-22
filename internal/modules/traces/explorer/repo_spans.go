package explorer

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

// SpanCountByService is a fallback raw-spans aggregator used when the
// traces_index path doesn't have the needed dim.
func (r *Repository) SpanCountByService(ctx context.Context, f querycompiler.Filters) ([]facetRowDTO, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetSpansRaw)
	query := fmt.Sprintf(
		`SELECT 'service' AS dim, service_name AS value, count() AS count
		 FROM %s WHERE %s GROUP BY service_name ORDER BY count DESC LIMIT 50`,
		spansRawTable, compiled.Where,
	)
	var rows []facetRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, compiled.Args...); err != nil {
		return nil, err
	}
	return rows, nil
}
