package explorer

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

// Facets returns top-N facet buckets per dim. Reads HLL-backed facet rollup
// for 7d+ windows; for smaller windows the rollup cascade suffices. Both
// paths source `logs_facets_rollup_5m` — its sumState/uniqHLL12State pair
// lets us run the same shape at any granularity.
func (r *Repository) Facets(ctx context.Context, f querycompiler.Filters) (Facets, []string, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetFacetRollup)
	rows, err := r.fetchFacetRows(ctx, compiled)
	if err != nil {
		return Facets{}, nil, err
	}
	return groupFacets(rows), compiled.DroppedClauses, nil
}

// fetchFacetRows unions facet dimensions from two backends:
//   - logs_facets_rollup_5m holds severity_bucket, service, environment as real
//     GROUP BY keys plus HLL sketches (not top-N facet values) for host/pod.
//   - logs_rollup_1m is used for host + pod facets: it is the only rollup
//     tier that defines `pod`, and the shared compileRollup WHERE clause may
//     reference `pod` whenever the user filters by pod — that predicate is
//     invalid on 5m/1h rollups (see db/clickhouse/17_rollup_logs.sql).
func (r *Repository) fetchFacetRows(ctx context.Context, compiled querycompiler.Compiled) ([]facetRowDTO, error) {
	// Facets still pin to 5m until the caller propagates startMs/endMs here
	// for proper tier selection — the rollup exists at 3 tiers but the facet
	// path has no time-window plumbing today.
	hostPodTbl := "observability." + logsRollupPrefix + "_1m"
	logsFacetRollupTbl := "observability." + logsFacetRollupPrefix + "_5m"

	// PREWHERE on (team_id, bucket_ts) leads the MergeTree sort key on
	// every rollup leg. Same predicates also live inside compiled.Where;
	// CH dedupes them at plan time.
	const pw = `PREWHERE team_id = @teamID AND bucket_ts BETWEEN @start AND @end`
	query := fmt.Sprintf(`
		SELECT dim, value, count
		FROM (
			SELECT 'severity_bucket' AS dim, toString(severity_bucket) AS value, sumMerge(log_count) AS count
			FROM %s `+pw+` WHERE %s GROUP BY severity_bucket
			UNION ALL
			SELECT 'service' AS dim, service AS value, sumMerge(log_count) AS count
			FROM %s `+pw+` WHERE %s GROUP BY service
			UNION ALL
			SELECT 'environment' AS dim, environment AS value, sumMerge(log_count) AS count
			FROM %s `+pw+` WHERE %s AND environment != '' GROUP BY environment
			UNION ALL
			SELECT 'host' AS dim, host AS value, sumMerge(log_count) AS count
			FROM %s `+pw+` WHERE %s AND host != '' GROUP BY host
			UNION ALL
			SELECT 'pod' AS dim, pod AS value, sumMerge(log_count) AS count
			FROM %s `+pw+` WHERE %s AND pod != '' GROUP BY pod
		) ORDER BY dim ASC, count DESC`,
		logsFacetRollupTbl, compiled.Where,
		logsFacetRollupTbl, compiled.Where,
		logsFacetRollupTbl, compiled.Where,
		hostPodTbl, compiled.Where,
		hostPodTbl, compiled.Where,
	)
	args := repeatArgs(compiled.Args, 5)
	var rows []facetRowDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "explorer.fetchFacetRows", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func groupFacets(rows []facetRowDTO) Facets {
	const topN = 50
	var out Facets
	for _, row := range rows {
		fv := FacetValue{Value: row.Value, Count: row.Count}
		switch row.Dim {
		case "severity_bucket":
			if len(out.Severity) < topN {
				out.Severity = append(out.Severity, fv)
			}
		case "service":
			if len(out.Service) < topN {
				out.Service = append(out.Service, fv)
			}
		case "host":
			if len(out.Host) < topN {
				out.Host = append(out.Host, fv)
			}
		case "pod":
			if len(out.Pod) < topN {
				out.Pod = append(out.Pod, fv)
			}
		case "environment":
			if len(out.Environment) < topN {
				out.Environment = append(out.Environment, fv)
			}
		}
	}
	return out
}

func repeatArgs(base []any, times int) []any {
	out := make([]any, 0, len(base)*times)
	for range times {
		out = append(out, base...)
	}
	return out
}
