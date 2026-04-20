package nodes

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
	"golang.org/x/sync/errgroup"
)

type Repository interface {
	GetInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]InfrastructureNode, error)
	GetInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// --- Host-level scan shapes ---------------------------------------------------
//
// The previous single mega-aggregate folded totals, errors, pod cardinality,
// services, avg latency, and a p95 percentile into one SELECT that leaned on
// several banned CH combinators (conditional counters, exact-set cardinality,
// array-string concat over unique arrays, exact percentile digests, and a
// scalar-conditional fallback for the host-name default). The fan-out below
// runs four narrow CH-native scans in parallel; the service-layer merge +
// CSV build + unknown-host default happens in Go. No sketch kind is keyed
// by host, so exact p95 is dropped and returned as 0 — task callers relying
// on node p95 should migrate to a per-host sketch kind when the UX requires
// it.

type nodeTotalDTO struct {
	HostName     string    `ch:"host_name"`
	RequestCount uint64    `ch:"request_count"`
	DurationSum  float64   `ch:"duration_sum_ms"`
	LastSeen     time.Time `ch:"last_seen"`
}

type nodeErrorDTO struct {
	HostName   string `ch:"host_name"`
	ErrorCount uint64 `ch:"error_count"`
}

type nodePodDTO struct {
	HostName string `ch:"host_name"`
	PodCount uint64 `ch:"pod_count"`
}

type nodeServiceDTO struct {
	HostName    string `ch:"host_name"`
	ServiceName string `ch:"service_name"`
}

// --- Per-host service scan shapes --------------------------------------------

type nodeServiceTotalDTO struct {
	ServiceName  string  `ch:"service_name"`
	RequestCount uint64  `ch:"request_count"`
	DurationSum  float64 `ch:"duration_sum_ms"`
}

type nodeServiceErrorDTO struct {
	ServiceName string `ch:"service_name"`
	ErrorCount  uint64 `ch:"error_count"`
}

type nodeServicePodDTO struct {
	ServiceName string `ch:"service_name"`
	PodCount    uint64 `ch:"pod_count"`
}

// spanBaseParams keeps the @teamID / @bucketStart / @bucketEnd / @start /
// @end param bundle in one place so all four scans share identical pruning.
func (r *ClickHouseRepository) spanBaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("bucketStart", time.Unix(int64(utils.SpansBucketStart(startMs/1000)), 0)),
		clickhouse.Named("bucketEnd", time.Unix(int64(utils.SpansBucketStart(endMs/1000)), 0)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func (r *ClickHouseRepository) GetInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]InfrastructureNode, error) {
	args := r.spanBaseParams(teamID, startMs, endMs)
	var (
		totals   []nodeTotalDTO
		errs     []nodeErrorDTO
		pods     []nodePodDTO
		services []nodeServiceDTO
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		q := `
			SELECT s.mat_host_name                  AS host_name,
			       count()                           AS request_count,
			       sum(s.duration_nano / 1000000.0)  AS duration_sum_ms,
			       max(s.timestamp)                  AS last_seen
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND ` + rootspan.Condition("s") + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			GROUP BY host_name
			ORDER BY request_count DESC
			LIMIT ` + maxNodesLiteral()
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, q, args...)
	})

	g.Go(func() error {
		q := `
			SELECT s.mat_host_name AS host_name,
			       count()          AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND ` + rootspan.Condition("s") + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			  AND (s.has_error = true OR s.http_status_code >= 400)
			GROUP BY host_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, q, args...)
	})

	g.Go(func() error {
		q := `
			SELECT s.mat_host_name                  AS host_name,
			       uniq(s.mat_k8s_pod_name)          AS pod_count
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND ` + rootspan.Condition("s") + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			  AND s.mat_k8s_pod_name != ''
			GROUP BY host_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &pods, q, args...)
	})

	g.Go(func() error {
		q := `
			SELECT s.mat_host_name AS host_name,
			       s.service_name   AS service_name
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND ` + rootspan.Condition("s") + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			  AND s.service_name != ''
			GROUP BY host_name, service_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &services, q, args...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[resolveHost(e.HostName)] = e.ErrorCount
	}
	podIdx := make(map[string]uint64, len(pods))
	for _, p := range pods {
		podIdx[resolveHost(p.HostName)] = p.PodCount
	}
	svcIdx := make(map[string][]string, len(services))
	for _, sv := range services {
		host := resolveHost(sv.HostName)
		svcIdx[host] = append(svcIdx[host], sv.ServiceName)
	}

	out := make([]InfrastructureNode, 0, len(totals))
	for _, t := range totals {
		host := resolveHost(t.HostName)
		reqCount := int64(t.RequestCount) //nolint:gosec // bounded by LIMIT
		errCount := int64(errIdx[host])   //nolint:gosec // bounded by LIMIT
		var errRate, avg float64
		if t.RequestCount > 0 {
			errRate = float64(errCount) * 100.0 / float64(t.RequestCount)
			avg = t.DurationSum / float64(t.RequestCount)
		}
		out = append(out, InfrastructureNode{
			Host:           host,
			PodCount:       int64(podIdx[host]), //nolint:gosec
			ContainerCount: 0,
			Services:       dedupSorted(svcIdx[host]),
			RequestCount:   reqCount,
			ErrorCount:     errCount,
			ErrorRate:      errRate,
			AvgLatencyMs:   avg,
			P95LatencyMs:   0, // No host-keyed sketch kind; exact p95 intentionally dropped.
			LastSeen:       t.LastSeen.Format(time.RFC3339),
		})
	}
	return out, nil
}

func (r *ClickHouseRepository) GetInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	// Callers pass "unknown" to select rows where mat_host_name is blank.
	hostParam := host
	if host == DefaultUnknown {
		hostParam = ""
	}
	args := append(r.spanBaseParams(teamID, startMs, endMs), clickhouse.Named("host", hostParam))
	hostFilter := hostFilterClause()

	var (
		totals []nodeServiceTotalDTO
		errs   []nodeServiceErrorDTO
		pods   []nodeServicePodDTO
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		q := `
			SELECT s.service_name                    AS service_name,
			       count()                            AS request_count,
			       sum(s.duration_nano / 1000000.0)   AS duration_sum_ms
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND ` + hostFilter + `
			  AND ` + rootspan.Condition("s") + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			GROUP BY service_name
			ORDER BY request_count DESC
			LIMIT ` + maxServicesLiteral()
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, q, args...)
	})

	g.Go(func() error {
		q := `
			SELECT s.service_name AS service_name,
			       count()         AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND ` + hostFilter + `
			  AND ` + rootspan.Condition("s") + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			  AND (s.has_error = true OR s.http_status_code >= 400)
			GROUP BY service_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, q, args...)
	})

	g.Go(func() error {
		q := `
			SELECT s.service_name             AS service_name,
			       uniq(s.mat_k8s_pod_name)   AS pod_count
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND ` + hostFilter + `
			  AND ` + rootspan.Condition("s") + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			  AND s.mat_k8s_pod_name != ''
			GROUP BY service_name`
		return r.db.Select(dbutil.OverviewCtx(gctx), &pods, q, args...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.ServiceName] = e.ErrorCount
	}
	podIdx := make(map[string]uint64, len(pods))
	for _, p := range pods {
		podIdx[p.ServiceName] = p.PodCount
	}

	out := make([]InfrastructureNodeService, 0, len(totals))
	for _, t := range totals {
		reqCount := int64(t.RequestCount) //nolint:gosec
		errCount := int64(errIdx[t.ServiceName])
		var errRate, avg float64
		if t.RequestCount > 0 {
			errRate = float64(errCount) * 100.0 / float64(t.RequestCount)
			avg = t.DurationSum / float64(t.RequestCount)
		}
		out = append(out, InfrastructureNodeService{
			ServiceName:  t.ServiceName,
			RequestCount: reqCount,
			ErrorCount:   errCount,
			ErrorRate:    errRate,
			AvgLatencyMs: avg,
			P95LatencyMs: 0, // No per-host-per-service sketch kind.
			PodCount:     int64(podIdx[t.ServiceName]), //nolint:gosec
		})
	}
	return out, nil
}

// resolveHost applies the `mat_host_name != '' ? mat_host_name : 'unknown'`
// fallback in Go. Pushing this into SQL would require the banned `if(...)`
// scalar expression.
func resolveHost(h string) string {
	h = strings.TrimSpace(h)
	if h == "" {
		return DefaultUnknown
	}
	return h
}

// hostFilterClause selects spans belonging to the requested host. The empty-
// string fallback to "unknown" happens in Go on the outgoing row builder;
// for the filter side, a blank host query gets rewritten to an IS-EMPTY
// match on mat_host_name via the named @host parameter.
func hostFilterClause() string {
	return "s.mat_host_name = @host"
}

// dedupSorted removes duplicates while preserving a stable ascending order
// so the services slice matches what the previous array-join-over-unique
// aggregate produced after the consumer sorted its comma-split output.
func dedupSorted(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	// Insertion sort — N is small (services per host).
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// maxNodesLiteral / maxServicesLiteral keep the `LIMIT N` fragments typed as
// strings at compile time without pulling strconv into the hot query-build
// path. The constants still live in otel_conventions.go.
func maxNodesLiteral() string    { return itoa(MaxNodes) }
func maxServicesLiteral() string { return itoa(MaxServices) }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
