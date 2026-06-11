package log_facets //nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
)

const facetTopN = 50

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// dimRow is the scan target for the unified facets query.
type dimRow struct {
	Dim   string `ch:"dim"`
	Value string `ch:"value"`
	Count uint64 `ch:"cnt"`
}

// Compute returns top-N values for all resource dimensions in one query.
func (r *Repository) Compute(ctx context.Context, f filter.Filters) ([]dimRow, error) {
	resourceWhere, _, args := filter.BuildClauses(f)
	args = append(args, clickhouse.Named("facetLimit", uint64(facetTopN)))

	// Each UNION arm shares the same PREWHERE shape on logs_resource.
	query := facetArm("service", resourceWhere) +
		" UNION ALL " + facetArm("host", resourceWhere) +
		" UNION ALL " + facetArm("pod", resourceWhere) +
		" UNION ALL " + facetArm("environment", resourceWhere)

	var rows []dimRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "logsFacets.Compute",
		&rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// facetArm builds a single UNION arm for the specified resource dimension.
func facetArm(dim, resourceWhere string) string {
	return `
		SELECT '` + dim + `' AS dim, ` + dim + ` AS value, count() AS cnt
		FROM observability.logs_resource
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		WHERE ` + dim + ` != ''
		GROUP BY ` + dim + `
		ORDER BY cnt DESC
		LIMIT @facetLimit`
}
