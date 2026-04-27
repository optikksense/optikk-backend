package explorer

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

type topKRow struct {
	TopServices	[]string	`ch:"top_services"`
	TopOperations	[]string	`ch:"top_operations"`
	TopHTTPMethods	[]string	`ch:"top_http_methods"`
	TopHTTPStatuses	[]string	`ch:"top_http_statuses"`
	TopStatuses	[]string	`ch:"top_statuses"`
}

// Facets returns counts per dim from traces_index for the given window.
// Keeps a bounded top-N per dim using a single table scan via topK aggregates.
func (r *Repository) Facets(ctx context.Context, f querycompiler.Filters) (Facets, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetSpansRaw)
	query := fmt.Sprintf(`
		SELECT
			topK(20)(service)                 AS top_services,
			topK(20)(name)                         AS top_operations,
			topK(10)(http_method)                  AS top_http_methods,
			topK(15)(toString(response_status_code)) AS top_http_statuses,
			topK(5)(status_code_string)            AS top_statuses
		FROM %s PREWHERE %s WHERE %s AND is_root = 1
	`, spansRawTable, compiled.PreWhere, compiled.Where)
	var rows []topKRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "explorer.Facets", &rows, query, compiled.Args...); err != nil {
		return Facets{}, err
	}
	if len(rows) == 0 {
		return Facets{}, nil
	}
	return pivotTopK(rows[0]), nil
}

func pivotTopK(row topKRow) Facets {
	toFacetBuckets := func(vals []string) []FacetBucket {
		out := make([]FacetBucket, 0, len(vals))
		for _, v := range vals {
			if v != "" {
				out = append(out, FacetBucket{Value: v})
			}
		}
		return out
	}
	return Facets{
		Service:	toFacetBuckets(row.TopServices),
		Operation:	toFacetBuckets(row.TopOperations),
		HTTPMethod:	toFacetBuckets(row.TopHTTPMethods),
		HTTPStatus:	toFacetBuckets(row.TopHTTPStatuses),
		Status:		toFacetBuckets(row.TopStatuses),
	}
}
