package log_facets //nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

type FacetRow struct {
	Service     string `ch:"service"`
	Host        string `ch:"host"`
	Pod         string `ch:"pod"`
	Environment string `ch:"environment"`
}

func (r *Repository) Facets(ctx context.Context, f filter.Filters) ([]FacetRow, error) {
	resourceWhere, _, args := filter.BuildClauses(f)
	query := `
		SELECT service, host, pod, environment
		FROM observability.logs_resource
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		LIMIT 50000`
	var rows []FacetRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "logsFacets.Facets",
		&rows, query, args...)
}
