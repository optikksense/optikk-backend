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

// dimRow is the scan target for the unified facets query. `dim` is the
// resource-dim discriminator (service/host/pod/environment); `value` is the
// dim value; `cnt` is the row count.
type dimRow struct {
	Dim   string `ch:"dim"`
	Value string `ch:"value"`
	Count uint64 `ch:"cnt"`
}

// Compute runs ONE CH query that returns top-N values for all four resource
// dims at once via UNION ALL on observability.logs_resource. Replaces the
// previous 5-goroutine fan-out (4 × TopValues + 1 × SeverityCounts). Severity
// is a closed enum returned statically by service.go — no DB call.
func (r *Repository) Compute(ctx context.Context, f filter.Filters) ([]dimRow, error) {
	resourceWhere, _, args := filter.BuildClauses(f)
	args = append(args, clickhouse.Named("facetLimit", uint64(facetTopN)))

	// Each UNION arm shares the same PREWHERE shape on logs_resource so the
	// CH planner can dedup the underlying granule reads. Even when it doesn't,
	// one round-trip + one plan compile beats five.
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

// facetArm builds one UNION arm. `dim` is from a closed set (service/host/
// pod/environment), so the inline column splice is safe. `resourceWhere` is
// emitted by filter.BuildClauses and contains only named-arg references.
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
