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

// FacetRow is the raw row scanned from CH for facet computation. The service
// folds these into per-dim top-N counts.
type FacetRow struct {
	SeverityBucket uint8  `ch:"severity_bucket"`
	Service        string `ch:"service"`
	Host           string `ch:"host"`
	Pod            string `ch:"pod"`
	Environment    string `ch:"environment"`
}

// Facets returns raw per-row dimension values for service-side fold + top-N.
func (r *Repository) Facets(ctx context.Context, f filter.Filters) ([]FacetRow, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.logs_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT severity_bucket, service, host, pod, environment
		FROM observability.logs
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end` + where
	var rows []FacetRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "logsFacets.Facets",
		&rows, query, args...)
}
