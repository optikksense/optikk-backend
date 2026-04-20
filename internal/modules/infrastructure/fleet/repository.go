package fleet

import (
	"context"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
	"golang.org/x/sync/errgroup"
)

const (
	maxFleetPods   = 200
	defaultUnknown = "unknown"
)

// fleetPodRow is the internal row returned from the repository. Percentiles are
// filled by the service layer via sketch.Querier (SpanLatencyService merged by
// the services present on each pod).
type fleetPodRow struct {
	PodName        string
	HostName       string
	Services       []string
	RequestCount   int64
	ErrorCount     int64
	ErrorRate      float64
	LatencyMsSum   float64
	LatencyMsCount int64
	P95Latency     float64
	LastSeen       time.Time
}

type Repository interface {
	GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]fleetPodRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// fleetAggRow is the top-level aggregate leg: one row per (pod_name, raw host)
// with request count, error count, latency sum+count, last-seen timestamp.
// has_error is already a Bool column on spans; http_status_code is the UInt16
// alias so the comparison stays plain. Error rate is derived Go-side from
// ErrorCount / RequestCount to keep the repository free of if()/division
// expressions in SELECT.
type fleetAggRow struct {
	PodName        string    `ch:"pod_name"`
	RawHostName    string    `ch:"raw_host_name"`
	RequestCount   uint64    `ch:"request_count"`
	ErrorCount     uint64    `ch:"error_count"`
	LatencyMsSum   float64   `ch:"latency_ms_sum"`
	LatencyMsCount uint64    `ch:"latency_ms_count"`
	LastSeen       time.Time `ch:"last_seen"`
}

// fleetServiceRow is the services-in-scope leg: a plain
// DISTINCT (pod_name, service_name) scan replacing the prior
// arrayStringConcat(groupUniqArray(...)) in-SELECT aggregation.
type fleetServiceRow struct {
	PodName     string `ch:"pod_name"`
	ServiceName string `ch:"service_name"`
}

func (r *ClickHouseRepository) GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]fleetPodRow, error) {
	params := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("bucketStart", time.Unix(int64(utils.SpansBucketStart(startMs/1000)), 0)),
		clickhouse.Named("bucketEnd", time.Unix(int64(utils.SpansBucketStart(endMs/1000)), 0)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var (
		aggRows     []fleetAggRow
		serviceRows []fleetServiceRow
	)
	g, gctx := errgroup.WithContext(ctx)

	// Leg 1: per-(pod, raw host) aggregates. Error count is filtered via an
	// AND predicate in WHERE on the inner scan, then counted alongside the
	// non-filtered total by summing a boolean-to-UInt8 comparison. The
	// boolean coercion keeps the emitted SQL free of -If combinators.
	g.Go(func() error {
		query := `
			SELECT pod_name, raw_host_name, request_count, error_count,
			       latency_ms_sum, latency_ms_count, last_seen
			FROM (
				SELECT s.mat_k8s_pod_name as pod_name,
				       s.mat_host_name    as raw_host_name,
				       count()            as request_count,
				       sum(s.has_error OR s.http_status_code >= 400) as error_count,
				       sum(s.duration_nano / 1000000.0) as latency_ms_sum,
				       count()            as latency_ms_count,
				       MAX(s.timestamp)   as last_seen
				FROM observability.spans s
				WHERE s.team_id = @teamID
				  AND ` + rootspan.Condition("s") + `
				  AND s.mat_k8s_pod_name != ''
				  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
				  AND s.timestamp BETWEEN @start AND @end
				GROUP BY pod_name, raw_host_name
				ORDER BY request_count DESC
				LIMIT ` + strconv.Itoa(maxFleetPods) + `
			)`
		return r.db.Select(dbutil.OverviewCtx(gctx), &aggRows, query, params...)
	})

	// Leg 2: distinct services per pod. Plain GROUP BY scan replaces the
	// groupUniqArray + arrayStringConcat materialisation.
	g.Go(func() error {
		query := `
			SELECT s.mat_k8s_pod_name as pod_name,
			       s.service_name     as service_name
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND ` + rootspan.Condition("s") + `
			  AND s.mat_k8s_pod_name != ''
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			GROUP BY pod_name, service_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &serviceRows, query, params...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Merge services into pod-keyed buckets.
	servicesByPod := make(map[string][]string, len(aggRows))
	for _, s := range serviceRows {
		if s.ServiceName == "" {
			continue
		}
		servicesByPod[s.PodName] = append(servicesByPod[s.PodName], s.ServiceName)
	}

	out := make([]fleetPodRow, len(aggRows))
	for i, a := range aggRows {
		host := a.RawHostName
		if host == "" {
			host = defaultUnknown
		}
		var errorRate float64
		if a.RequestCount > 0 {
			errorRate = float64(a.ErrorCount) * 100.0 / float64(a.RequestCount)
		}
		out[i] = fleetPodRow{
			PodName:        a.PodName,
			HostName:       host,
			Services:       servicesByPod[a.PodName],
			RequestCount:   int64(a.RequestCount),   //nolint:gosec // G115
			ErrorCount:     int64(a.ErrorCount),     //nolint:gosec // G115
			ErrorRate:      errorRate,
			LatencyMsSum:   a.LatencyMsSum,
			LatencyMsCount: int64(a.LatencyMsCount), //nolint:gosec // G115
			P95Latency:     0,
			LastSeen:       a.LastSeen,
		}
	}
	return out, nil
}
