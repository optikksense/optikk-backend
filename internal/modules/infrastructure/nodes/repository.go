package nodes

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

type Repository interface {
	GetInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]infrastructureNodeRecordDTO, error)
	GetInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]infrastructureNodeServiceRecordDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type infrastructureNodeDTO struct {
	HostName       string    `ch:"host_name"`
	PodCount       int64     `ch:"pod_count"`
	ContainerCount int64     `ch:"container_count"`
	ServicesCSV    string    `ch:"services_csv"`
	RequestCount   int64     `ch:"request_count"`
	ErrorCount     int64     `ch:"error_count"`
	ErrorRate      float64   `ch:"error_rate"`
	AvgLatency     float64   `ch:"avg_latency"`
	P95Latency     float64   `ch:"p95_latency"`
	LastSeen       time.Time `ch:"last_seen"`
}

type infrastructureNodeServiceDTO struct {
	ServiceName  string  `ch:"service_name"`
	RequestCount int64   `ch:"request_count"`
	ErrorCount   int64   `ch:"error_count"`
	ErrorRate    float64 `ch:"error_rate"`
	AvgLatency   float64 `ch:"avg_latency"`
	P95Latency   float64 `ch:"p95_latency"`
	PodCount     int64   `ch:"pod_count"`
}

func (r *ClickHouseRepository) GetInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]InfrastructureNode, error) {
	query := `
		SELECT if(s.mat_host_name != '', s.mat_host_name, '` + DefaultUnknown + `') as host_name,
		       toInt64(uniqIf(s.mat_k8s_pod_name, s.mat_k8s_pod_name != '')) as pod_count,
		       toInt64(0) as container_count,
		       arrayStringConcat(groupUniqArray(s.service_name), ',') as services_csv,
		       toInt64(COUNT(*)) as request_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) as error_count,
		       if(COUNT(*) > 0, countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)*100.0/COUNT(*), 0) as error_rate,
		       AVG(s.duration_nano / 1000000.0) as avg_latency,
		       quantileTDigest(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) as p95_latency,
		       MAX(s.timestamp) as last_seen
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND ` + rootspan.Condition("s") + `
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		GROUP BY host_name
		ORDER BY request_count DESC
		LIMIT ` + strconv.Itoa(MaxNodes)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)),                                                 //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("bucketStart", time.Unix(int64(utils.SpansBucketStart(startMs/1000)), 0)), //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("bucketEnd", time.Unix(int64(utils.SpansBucketStart(endMs/1000)), 0)),     //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var dtos []infrastructureNodeDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, params...); err != nil {
		return nil, err
	}

	nodes := make([]InfrastructureNode, len(dtos))
	for i, d := range dtos {
		nodes[i] = InfrastructureNode{
			Host:           d.HostName,
			PodCount:       d.PodCount,
			ContainerCount: d.ContainerCount,
			Services:       splitCSV(d.ServicesCSV),
			RequestCount:   d.RequestCount,
			ErrorCount:     d.ErrorCount,
			ErrorRate:      d.ErrorRate,
			AvgLatencyMs:   d.AvgLatency,
			P95LatencyMs:   d.P95Latency,
			LastSeen:       d.LastSeen.Format(time.RFC3339),
		}
	}
	return nodes, nil
}

func (r *ClickHouseRepository) GetInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	query := `
		SELECT s.service_name as service_name,
		       toInt64(COUNT(*)) as request_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) as error_count,
		       if(COUNT(*) > 0, countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)*100.0/COUNT(*), 0) as error_rate,
		       AVG(s.duration_nano / 1000000.0) as avg_latency,
		       quantileTDigest(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) as p95_latency,
		       toInt64(uniqIf(s.mat_k8s_pod_name, s.mat_k8s_pod_name != '')) as pod_count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND if(s.mat_host_name != '', s.mat_host_name, '` + DefaultUnknown + `') = @host
		  AND ` + rootspan.Condition("s") + `
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		GROUP BY service_name
		ORDER BY request_count DESC
		LIMIT ` + strconv.Itoa(MaxServices)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("host", host),
		clickhouse.Named("bucketStart", time.Unix(int64(utils.SpansBucketStart(startMs/1000)), 0)), //nolint:gosec // G115
		clickhouse.Named("bucketEnd", time.Unix(int64(utils.SpansBucketStart(endMs/1000)), 0)),     //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var dtos []infrastructureNodeServiceDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &dtos, query, params...); err != nil {
		return nil, err
	}

	services := make([]InfrastructureNodeService, len(dtos))
	for i, d := range dtos {
		services[i] = InfrastructureNodeService{
			ServiceName:  d.ServiceName,
			RequestCount: d.RequestCount,
			ErrorCount:   d.ErrorCount,
			ErrorRate:    d.ErrorRate,
			AvgLatencyMs: d.AvgLatency,
			P95LatencyMs: d.P95Latency,
			PodCount:     d.PodCount,
		}
	}
	return services, nil
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
