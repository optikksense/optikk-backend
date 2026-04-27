package log_facets //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"sort"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/resource"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// Facets returns top-N facet buckets reduced in Go after PREWHERE-pruned raw scans.
func (r *Repository) Facets(ctx context.Context, f querycompiler.Filters) (models.Facets, []string, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetRaw)
	preWhere, args, empty, err := resource.WithFingerprints(ctx, r.db, f, compiled.PreWhere, compiled.Args)
	if err != nil {
		return models.Facets{}, nil, err
	}
	if empty {
		return models.Facets{}, compiled.DroppedClauses, nil
	}

	rows, err := dbutil.QueryCH(dbutil.OverviewCtx(ctx), r.db, "logsFacets.Facets", fmt.Sprintf(`
		SELECT severity_bucket, service, host, pod, environment
		FROM %s
		PREWHERE %s
		WHERE %s
	`, models.RawLogsTable, preWhere, compiled.Where), args...)
	if err != nil {
		return models.Facets{}, nil, err
	}
	defer rows.Close() //nolint:errcheck

	severity := make(map[string]uint64)
	service := make(map[string]uint64)
	host := make(map[string]uint64)
	pod := make(map[string]uint64)
	environment := make(map[string]uint64)

	for rows.Next() {
		var sev uint8
		var svc, h, p, env string
		if err := rows.Scan(&sev, &svc, &h, &p, &env); err != nil {
			return models.Facets{}, nil, err
		}
		severity[fmt.Sprintf("%d", sev)]++
		if svc != "" {
			service[svc]++
		}
		if h != "" {
			host[h]++
		}
		if p != "" {
			pod[p]++
		}
		if env != "" {
			environment[env]++
		}
	}
	if err := rows.Err(); err != nil {
		return models.Facets{}, nil, err
	}

	return models.Facets{
		Severity:    topFacetValues(severity, 50),
		Service:     topFacetValues(service, 50),
		Host:        topFacetValues(host, 50),
		Pod:         topFacetValues(pod, 50),
		Environment: topFacetValues(environment, 50),
	}, compiled.DroppedClauses, nil
}

func topFacetValues(counts map[string]uint64, limit int) []models.FacetValue {
	type pair struct {
		value string
		count uint64
	}
	pairs := make([]pair, 0, len(counts))
	for value, count := range counts {
		pairs = append(pairs, pair{value: value, count: count})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count == pairs[j].count {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].count > pairs[j].count
	})
	if len(pairs) > limit {
		pairs = pairs[:limit]
	}
	out := make([]models.FacetValue, 0, len(pairs))
	for _, pair := range pairs {
		out = append(out, models.FacetValue{Value: pair.value, Count: pair.count})
	}
	return out
}
