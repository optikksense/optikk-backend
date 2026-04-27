package nodes

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	)

// Reads run against raw `observability.spans` (the planned
// spans_host_rollup_{1m,5m,1h} cascade was never built). Per-host
// RED metrics are computed inline; pod + service cardinality comes from
// `uniq()` on the grouping dims, with LIMIT to bound the scan.
const spansHostRollupPrefix = "observability.spans"

// queryIntervalMinutes returns max(tierStep, dashboardStep) so the query-time
// step is never finer than the tier's native resolution. Matches the helper
// in overview/overview/repository.go.
func queryIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var dashStep int64
	switch {
	case hours <= 3:
		dashStep = 1
	case hours <= 24:
		dashStep = 5
	case hours <= 168:
		dashStep = 60
	default:
		dashStep = 1440
	}
	if tierStepMin > dashStep {
		return tierStepMin
	}
	return dashStep
}

type Repository interface {
	GetInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]InfrastructureNode, error)
	GetInfrastructureNodeSummary(ctx context.Context, teamID int64, startMs, endMs int64) (InfrastructureNodeSummary, error)
	GetInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type infrastructureNodeDTO struct {
	HostName	string		`ch:"host"`
	PodCount	uint64		`ch:"pod_count"`
	ServicesCSV	string		`ch:"services_csv"`
	RequestCount	uint64		`ch:"request_count"`
	ErrorCount	uint64		`ch:"error_count"`
	DurationMsSum	float64		`ch:"duration_ms_sum"`
	P95Latency	float64		`ch:"p95_latency"`
	LastSeen	time.Time	`ch:"last_seen"`
}

type infrastructureNodeServiceDTO struct {
	ServiceName	string	`ch:"service"`
	RequestCount	uint64	`ch:"request_count"`
	ErrorCount	uint64	`ch:"error_count"`
	DurationMsSum	float64	`ch:"duration_ms_sum"`
	P95Latency	float64	`ch:"p95_latency"`
	PodCount	uint64	`ch:"pod_count"`
}

func (r *ClickHouseRepository) GetInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]InfrastructureNode, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT if(host != '', host, '%s') AS host,
		       uniqIf(pod, pod != '')                                  AS pod_count,
		       count()                                           AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                                             AS error_count,
		       sum(duration_nano / 1000000.0)                                         AS duration_ms_sum,
		       max(ts_bucket)                                                    AS last_seen
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY host
		ORDER BY request_count DESC
		LIMIT `+strconv.Itoa(MaxNodes), DefaultUnknown, table)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115 - domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var dtos []infrastructureNodeDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "nodes.GetInfrastructureNodes", &dtos, query, params...); err != nil {
		return nil, err
	}

	nodes := make([]InfrastructureNode, len(dtos))
	for i, d := range dtos {
		errorRate := 0.0
		if d.RequestCount > 0 {
			errorRate = float64(d.ErrorCount) * 100.0 / float64(d.RequestCount)
		}
		avgLatency := 0.0
		if d.RequestCount > 0 {
			avgLatency = d.DurationMsSum / float64(d.RequestCount)
		}
		nodes[i] = InfrastructureNode{
			Host:		d.HostName,
			PodCount:	int64(d.PodCount),	//nolint:gosec // domain-bounded
			ContainerCount:	0,			// rollup does not carry container cardinality
			Services:	[]string{},		// no longer fetched for fleet view performance
			RequestCount:	int64(d.RequestCount),
			ErrorCount:	int64(d.ErrorCount),
			ErrorRate:	errorRate,
			AvgLatencyMs:	avgLatency,
			P95LatencyMs:	0,	// no longer fetched for fleet view performance
			LastSeen:	d.LastSeen.Format(time.RFC3339),
		}
	}
	return nodes, nil
}

func (r *ClickHouseRepository) GetInfrastructureNodeSummary(ctx context.Context, teamID int64, startMs, endMs int64) (InfrastructureNodeSummary, error) {
	table := "observability.spans"
	// Dedicated summary query that avoids the MaxNodes limit and executes a
	// single pass over host-level aggregates to categorize health.
	query := fmt.Sprintf(`
		SELECT
		    toInt64(countIf(error_rate > 10))                        AS unhealthy_nodes,
		    toInt64(countIf(error_rate > 2 AND error_rate <= 10))    AS degraded_nodes,
		    toInt64(countIf(error_rate <= 2))                        AS healthy_nodes,
		    toInt64(sum(pod_count))                                  AS total_pods
		FROM (
		    SELECT
		        host,
		        (countIf(has_error OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / nullIf(toFloat64(count()), 0)) AS error_rate,
		        uniqMerge(pod_count)                                                         AS pod_count
		    FROM %s
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @start AND @end
		    GROUP BY host
		)`, table)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var row struct {
		HealthyNodes	int64	`ch:"healthy_nodes"`
		DegradedNodes	int64	`ch:"degraded_nodes"`
		UnhealthyNodes	int64	`ch:"unhealthy_nodes"`
		TotalPods	*int64	`ch:"total_pods"`
	}

	if err := dbutil.QueryRowCH(dbutil.DashboardCtx(ctx), r.db, "nodes.GetInfrastructureNodeSummary", &row, query, params...); err != nil {
		return InfrastructureNodeSummary{}, err
	}

	var totalPods int64
	if row.TotalPods != nil {
		totalPods = *row.TotalPods
	}

	return InfrastructureNodeSummary{
		HealthyNodes:	row.HealthyNodes,
		DegradedNodes:	row.DegradedNodes,
		UnhealthyNodes:	row.UnhealthyNodes,
		TotalPods:	totalPods,
	}, nil
}

func (r *ClickHouseRepository) GetInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT service                                                      AS service,
		       count()                                           AS request_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)                                             AS error_count,
		       sum(duration_nano / 1000000.0)                                         AS duration_ms_sum,
		       toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) AS p95_latency,
		       uniqIf(pod, pod != '')                                  AS pod_count
		FROM %s
		WHERE team_id = @teamID
		  AND if(host != '', host, '%s') = @host
		  AND ts_bucket BETWEEN @start AND @end
		GROUP BY service
		ORDER BY request_count DESC
		LIMIT `+strconv.Itoa(MaxServices), table, DefaultUnknown)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("host", host),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var dtos []infrastructureNodeServiceDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "nodes.GetInfrastructureNodeServices", &dtos, query, params...); err != nil {
		return nil, err
	}

	services := make([]InfrastructureNodeService, len(dtos))
	for i, d := range dtos {
		errorRate := 0.0
		if d.RequestCount > 0 {
			errorRate = float64(d.ErrorCount) * 100.0 / float64(d.RequestCount)
		}
		avgLatency := 0.0
		if d.RequestCount > 0 {
			avgLatency = d.DurationMsSum / float64(d.RequestCount)
		}
		services[i] = InfrastructureNodeService{
			ServiceName:	d.ServiceName,
			RequestCount:	int64(d.RequestCount),	//nolint:gosec // domain-bounded
			ErrorCount:	int64(d.ErrorCount),	//nolint:gosec // domain-bounded
			ErrorRate:	errorRate,
			AvgLatencyMs:	avgLatency,
			P95LatencyMs:	d.P95Latency,
			PodCount:	int64(d.PodCount),	//nolint:gosec // domain-bounded
		}
	}
	return services, nil
}

// splitCSV splits the CSV list of service names returned by
// arrayStringConcat(groupUniqArray(...), ',').
func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	parts := strings.Split(s, ",")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "'\"")
		if p != "" {
			clean = append(clean, p)
		}
	}
	return clean
}
