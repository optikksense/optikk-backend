package fleet

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
)

// Reads target the `observability.spans_host_rollup_{1m,5m,1h}` cascade —
// Phase 6 per-pod RED aggregates. Rollup keyed by
// (team_id, bucket_ts, host_name, pod_name, service_name).
const (
	spansHostRollupPrefix = "observability.spans_host_rollup"
	maxFleetPods          = 200
	defaultUnknown        = "unknown"
)

// queryIntervalMinutes returns max(tierStep, dashboardStep). Matches the
// helper in overview/overview/repository.go.
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
	GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPod, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type fleetPodRowDTO struct {
	PodName       string    `ch:"pod_name"`
	HostName      string    `ch:"host_name"`
	ServicesCSV   string    `ch:"services_csv"`
	RequestCount  uint64    `ch:"request_count"`
	ErrorCount    uint64    `ch:"error_count"`
	DurationMsSum float64   `ch:"duration_ms_sum"`
	P95Latency    float64   `ch:"p95_latency"`
	LastSeen      time.Time `ch:"last_seen"`
}

func (r *ClickHouseRepository) GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPod, error) {
	table, _ := rollup.TierTableFor(spansHostRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT pod_name                                                          AS pod_name,
		       if(host_name != '', host_name, '%s')                              AS host_name,
		       arrayStringConcat(groupUniqArray(service_name), ',')              AS services_csv,
		       sumMerge(request_count)                                           AS request_count,
		       sumMerge(error_count)                                             AS error_count,
		       sumMerge(duration_ms_sum)                                         AS duration_ms_sum,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_latency,
		       max(bucket_ts)                                                    AS last_seen
		FROM %s
		WHERE team_id = @teamID
		  AND pod_name != ''
		  AND bucket_ts BETWEEN @start AND @end
		GROUP BY pod_name, host_name
		ORDER BY request_count DESC
		LIMIT `+strconv.Itoa(maxFleetPods), defaultUnknown, table)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var dtos []fleetPodRowDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, params...); err != nil {
		return nil, err
	}

	out := make([]FleetPod, len(dtos))
	for i, d := range dtos {
		errorRate := 0.0
		if d.RequestCount > 0 {
			errorRate = float64(d.ErrorCount) * 100.0 / float64(d.RequestCount)
		}
		avgLatency := 0.0
		if d.RequestCount > 0 {
			avgLatency = d.DurationMsSum / float64(d.RequestCount)
		}
		out[i] = FleetPod{
			PodName:      d.PodName,
			Host:         d.HostName,
			Services:     splitCSV(d.ServicesCSV),
			RequestCount: int64(d.RequestCount), //nolint:gosec // domain-bounded
			ErrorCount:   int64(d.ErrorCount),   //nolint:gosec // domain-bounded
			ErrorRate:    errorRate,
			AvgLatencyMs: avgLatency,
			P95LatencyMs: d.P95Latency,
			LastSeen:     d.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}

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
