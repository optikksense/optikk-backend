package explorer

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

// Facets returns counts per dim from traces_index for the given window.
// Keeps a bounded top-N per dim; callers that need the full set should
// hit spansRawTable via SpanCountByService.
func (r *Repository) Facets(ctx context.Context, f querycompiler.Filters) (Facets, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetTracesIndex)
	query := fmt.Sprintf(`
		SELECT dim, value, count FROM (
			SELECT 'service' AS dim, root_service AS value, count() AS count FROM %[1]s WHERE %[2]s GROUP BY root_service ORDER BY count DESC LIMIT 20
			UNION ALL
			SELECT 'operation' AS dim, root_operation AS value, count() AS count FROM %[1]s WHERE %[2]s GROUP BY root_operation ORDER BY count DESC LIMIT 20
			UNION ALL
			SELECT 'http_method' AS dim, root_http_method AS value, count() AS count FROM %[1]s WHERE %[2]s GROUP BY root_http_method ORDER BY count DESC LIMIT 10
			UNION ALL
			SELECT 'http_status' AS dim, root_http_status AS value, count() AS count FROM %[1]s WHERE %[2]s GROUP BY root_http_status ORDER BY count DESC LIMIT 15
			UNION ALL
			SELECT 'status' AS dim, root_status AS value, count() AS count FROM %[1]s WHERE %[2]s GROUP BY root_status ORDER BY count DESC LIMIT 5
		)
	`, tracesIndexTable, compiled.Where)
	args := compiled.Args
	// CH resolves named args per occurrence; we repeat the same names.
	var rows []facetRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return Facets{}, err
	}
	return pivotFacets(rows), nil
}

func pivotFacets(rows []facetRowDTO) Facets {
	var f Facets
	for _, r := range rows {
		b := FacetBucket{Value: r.Value, Count: r.Count}
		switch r.Dim {
		case "service":
			f.Service = append(f.Service, b)
		case "operation":
			f.Operation = append(f.Operation, b)
		case "http_method":
			f.HTTPMethod = append(f.HTTPMethod, b)
		case "http_status":
			f.HTTPStatus = append(f.HTTPStatus, b)
		case "status":
			f.Status = append(f.Status, b)
		}
	}
	return f
}
