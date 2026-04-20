package fleet

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
)

const (
	maxFleetPods   = 200
	defaultUnknown = "unknown"
)

// fleetPodRow is the internal row returned from the repository. Percentiles are
// filled by the service layer via sketch.Querier (SpanLatencyService merged by
// the services present on each pod).
type fleetPodRow struct {
	PodName         string
	HostName        string
	Services        []string
	RequestCount    int64
	ErrorCount      int64
	ErrorRate       float64
	LatencyMsSum    float64
	LatencyMsCount  int64
	P95Latency      float64
	LastSeen        time.Time
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

type fleetPodRowDTO struct {
	PodName        string    `ch:"pod_name"`
	HostName       string    `ch:"host_name"`
	ServicesCSV    string    `ch:"services_csv"`
	RequestCount   int64     `ch:"request_count"`
	ErrorCount     int64     `ch:"error_count"`
	ErrorRate      float64   `ch:"error_rate"`
	LatencyMsSum   float64   `ch:"latency_ms_sum"`
	LatencyMsCount int64     `ch:"latency_ms_count"`
	P95Latency     float64   `ch:"p95_latency"`
	LastSeen       time.Time `ch:"last_seen"`
}

func (r *ClickHouseRepository) GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]fleetPodRow, error) {
	// Percentiles come from sketch.Querier (SpanLatencyService merged across
	// the services attached to each pod) — see service.go. avg latency is
	// computed Go-side from sum/count to keep the repository free of avg().
	query := `
		SELECT pod_name, host_name, services_csv, request_count, error_count, error_rate,
		       latency_ms_sum, latency_ms_count,
		       0 AS p95_latency,
		       last_seen
		FROM (
			SELECT s.mat_k8s_pod_name as pod_name,
			       if(s.mat_host_name != '', s.mat_host_name, '` + defaultUnknown + `') as host_name,
			       arrayStringConcat(groupUniqArray(s.service_name), ',') as services_csv,
			       toInt64(COUNT(*)) as request_count,
			       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) as error_count,
			       if(COUNT(*) > 0, countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)*100.0/COUNT(*), 0) as error_rate,
			       sum(s.duration_nano / 1000000.0) as latency_ms_sum,
			       toInt64(count()) as latency_ms_count,
			       MAX(s.timestamp) as last_seen
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND ` + rootspan.Condition("s") + `
			  AND s.mat_k8s_pod_name != ''
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			GROUP BY pod_name, host_name
			ORDER BY request_count DESC
			LIMIT ` + strconv.Itoa(maxFleetPods) + `
		)`

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("bucketStart", time.Unix(int64(utils.SpansBucketStart(startMs/1000)), 0)), //nolint:gosec // G115
		clickhouse.Named("bucketEnd", time.Unix(int64(utils.SpansBucketStart(endMs/1000)), 0)),     //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var dtos []fleetPodRowDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, params...); err != nil {
		return nil, err
	}

	out := make([]fleetPodRow, len(dtos))
	for i, d := range dtos {
		out[i] = fleetPodRow{
			PodName:        d.PodName,
			HostName:       d.HostName,
			Services:       splitCSV(d.ServicesCSV),
			RequestCount:   d.RequestCount,
			ErrorCount:     d.ErrorCount,
			ErrorRate:      d.ErrorRate,
			LatencyMsSum:   d.LatencyMsSum,
			LatencyMsCount: d.LatencyMsCount,
			P95Latency:     d.P95Latency,
			LastSeen:       d.LastSeen,
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
