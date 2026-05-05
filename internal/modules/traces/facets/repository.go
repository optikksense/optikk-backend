package facets

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// Facets returns top-N values per dim over the filtered window via topK
// aggregates in a single scan against observability.spans_1m.
func (r *Repository) Facets(ctx context.Context, f filter.Filters) (Facets, error) {
	resourceWhere, where, args := filter.BuildClauses(f)

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT topK(20)(service)              AS top_services,
		       topK(20)(name)                 AS top_operations,
		       topK(10)(http_method)          AS top_http_methods,
		       topK(15)(response_status_code) AS top_http_statuses,
		       topK(5)(status_code_string)    AS top_statuses
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end AND is_root = 1` + where

	var rows []topKRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "facets.Facets", &rows, query, args...); err != nil {
		return Facets{}, err
	}
	if len(rows) == 0 {
		return Facets{}, nil
	}
	return pivotTopK(rows[0]), nil
}

// pivotTopK turns CH topK array results into FacetBucket lists. Empty values
// are dropped.
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
		Service:    toFacetBuckets(row.TopServices),
		Operation:  toFacetBuckets(row.TopOperations),
		HTTPMethod: toFacetBuckets(row.TopHTTPMethods),
		HTTPStatus: toFacetBuckets(row.TopHTTPStatuses),
		Status:     toFacetBuckets(row.TopStatuses),
	}
}
