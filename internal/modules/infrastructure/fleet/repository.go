package fleet

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
)

const (
	quantileP95   = 0.95
	maxFleetPods  = 200
	defaultUnknown = "unknown"
)

type Repository interface {
	GetFleetPods(teamID int64, startMs, endMs int64) ([]FleetPod, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type fleetPodRowDTO struct {
	PodName      string    `ch:"pod_name"`
	HostName     string    `ch:"host_name"`
	ServicesCSV  string    `ch:"services_csv"`
	RequestCount int64     `ch:"request_count"`
	ErrorCount   int64     `ch:"error_count"`
	ErrorRate    float64   `ch:"error_rate"`
	AvgLatency   float64   `ch:"avg_latency"`
	P95Latency   float64   `ch:"p95_latency"`
	LastSeen     time.Time `ch:"last_seen"`
}

func (r *ClickHouseRepository) GetFleetPods(teamID int64, startMs, endMs int64) ([]FleetPod, error) {
	query := `
		SELECT s.mat_k8s_pod_name as pod_name,
		       if(s.mat_host_name != '', s.mat_host_name, '` + defaultUnknown + `') as host_name,
		       arrayStringConcat(groupUniqArray(s.service_name), ',') as services_csv,
		       toInt64(COUNT(*)) as request_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) as error_count,
		       if(COUNT(*) > 0, countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)*100.0/COUNT(*), 0) as error_rate,
		       AVG(s.duration_nano / 1000000.0) as avg_latency,
		       quantile(` + fmt.Sprintf("%.2f", quantileP95) + `)(s.duration_nano / 1000000.0) as p95_latency,
		       MAX(s.timestamp) as last_seen
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND ` + rootspan.Condition("s") + `
		  AND s.mat_k8s_pod_name != ''
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		GROUP BY pod_name, host_name
		ORDER BY request_count DESC
		LIMIT ` + strconv.Itoa(maxFleetPods)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("bucketStart", time.Unix(int64(utils.SpansBucketStart(startMs/1000)), 0)), //nolint:gosec // G115
		clickhouse.Named("bucketEnd", time.Unix(int64(utils.SpansBucketStart(endMs/1000)), 0)),     //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var dtos []fleetPodRowDTO
	if err := r.db.Select(context.Background(), &dtos, query, params...); err != nil {
		return nil, err
	}

	out := make([]FleetPod, len(dtos))
	for i, d := range dtos {
		out[i] = FleetPod{
			PodName:      d.PodName,
			Host:         d.HostName,
			Services:     splitCSV(d.ServicesCSV),
			RequestCount: d.RequestCount,
			ErrorCount:   d.ErrorCount,
			ErrorRate:    d.ErrorRate,
			AvgLatencyMs: d.AvgLatency,
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
