package log_facets //nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

const facetTopN = 50

// resourceDimColumns is the closed set of columns that may be string-concat'd
// into a SELECT/GROUP BY on observability.logs_resource. Keeping this set
// closed (never user-supplied) is what makes the inline column splice safe
// without fmt.Sprintf-style escaping. Same pattern traces/trace_suggest uses.
var resourceDimColumns = map[string]bool{
	"service": true, "host": true, "pod": true, "environment": true,
}

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// dimRow is the scan target for the 4 resource-dim facet queries. The single
// `value` column is whatever dim we asked CH to GROUP BY.
type dimRow struct {
	Value string `ch:"value"`
	Count uint64 `ch:"cnt"`
}

// severityRow is the scan target for the severity facet query.
type severityRow struct {
	Bucket uint8  `ch:"severity_bucket"`
	Count  uint64 `ch:"cnt"`
}

// TopValues returns the top-N values for a single resource dim
// (service / host / pod / environment) as `[]FacetValue`. Each query is a
// small GROUP BY against observability.logs_resource — the dictionary table
// is orders of magnitude smaller than raw logs.
func (r *Repository) TopValues(ctx context.Context, f filter.Filters, dim string) ([]models.FacetValue, error) {
	if !resourceDimColumns[dim] {
		return nil, nil
	}
	resourceWhere, _, args := filter.BuildClauses(f)
	query := `
		SELECT ` + dim + ` AS value, count() AS cnt
		FROM observability.logs_resource
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		WHERE ` + dim + ` != ''
		GROUP BY ` + dim + `
		ORDER BY cnt DESC
		LIMIT @facetLimit`
	args = append(args, clickhouse.Named("facetLimit", uint64(facetTopN)))

	var rows []dimRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "logsFacets.TopValues."+dim,
		&rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]models.FacetValue, len(rows))
	for i, row := range rows {
		out[i] = models.FacetValue{Value: row.Value, Count: row.Count}
	}
	return out, nil
}

// SeverityCounts returns the per-severity-bucket count for the filter window.
// Severity isn't a resource attribute, so this query targets raw logs — but
// the result set is bounded to 6 rows (severity tiers 0..5), so wire payload
// is tiny.
func (r *Repository) SeverityCounts(ctx context.Context, f filter.Filters) ([]models.FacetValue, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.logs_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT severity_bucket, count() AS cnt
		FROM observability.logs
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end` + where + `
		GROUP BY severity_bucket
		ORDER BY severity_bucket ASC`

	var rows []severityRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "logsFacets.SeverityCounts",
		&rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]models.FacetValue, len(rows))
	for i, row := range rows {
		out[i] = models.FacetValue{Value: severityBucketLabel(row.Bucket), Count: row.Count}
	}
	return out, nil
}

// severityBucketLabel mirrors the on-disk severity_bucket → text mapping
// (0=TRACE/UNSET, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR, 5=FATAL).
func severityBucketLabel(b uint8) string {
	switch b {
	case 0:
		return "UNSET"
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARN"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	return ""
}
