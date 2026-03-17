package nodes

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	database "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetInfrastructureNodes(teamID int64, startMs, endMs int64) ([]infrastructureNodeRecordDTO, error)
	GetInfrastructureNodeServices(teamID int64, host string, startMs, endMs int64) ([]infrastructureNodeServiceRecordDTO, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
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

func (r *ClickHouseRepository) GetInfrastructureNodes(teamID int64, startMs, endMs int64) ([]InfrastructureNode, error) {
	const rootSpanCondition = "(s.parent_span_id = '' OR s.parent_span_id = '0000000000000000')"

	query := `
		SELECT if(s.mat_host_name != '', s.mat_host_name, '` + DefaultUnknown + `') as host_name,
		       uniqExactIf(s.mat_k8s_pod_name, s.mat_k8s_pod_name != '') as pod_count,
		       toInt64(0) as container_count,
		       arrayStringConcat(groupUniqArray(s.service_name), ',') as services_csv,
		       COUNT(*) as request_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) as error_count,
		       if(COUNT(*) > 0, countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)*100.0/COUNT(*), 0) as error_rate,
		       AVG(s.duration_nano / 1000000.0) as avg_latency,
		       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) as p95_latency,
		       MAX(s.timestamp) as last_seen
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND ` + rootSpanCondition + `
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		GROUP BY host_name
		ORDER BY request_count DESC
		LIMIT ` + fmt.Sprintf("%d", MaxNodes)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", time.Unix(int64(timebucket.SpansBucketStart(startMs/1000)), 0)),
		clickhouse.Named("bucketEnd", time.Unix(int64(timebucket.SpansBucketStart(endMs/1000)), 0)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var dtos []infrastructureNodeDTO
	if err := r.db.Select(context.Background(), &dtos, query, params...); err != nil {
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

func (r *ClickHouseRepository) GetInfrastructureNodeServices(teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	const rootSpanCondition = "(s.parent_span_id = '' OR s.parent_span_id = '0000000000000000')"

	query := `
		SELECT s.service_name as service_name,
		       COUNT(*) as request_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) as error_count,
		       if(COUNT(*) > 0, countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)*100.0/COUNT(*), 0) as error_rate,
		       AVG(s.duration_nano / 1000000.0) as avg_latency,
		       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) as p95_latency,
		       uniqExactIf(s.mat_k8s_pod_name, s.mat_k8s_pod_name != '') as pod_count
		FROM observability.spans s
		WHERE s.team_id = @teamID
		  AND if(s.mat_host_name != '', s.mat_host_name, '` + DefaultUnknown + `') = @host
		  AND ` + rootSpanCondition + `
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		GROUP BY service_name
		ORDER BY request_count DESC
		LIMIT ` + fmt.Sprintf("%d", MaxServices)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("host", host),
		clickhouse.Named("bucketStart", time.Unix(int64(timebucket.SpansBucketStart(startMs/1000)), 0)),
		clickhouse.Named("bucketEnd", time.Unix(int64(timebucket.SpansBucketStart(endMs/1000)), 0)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	var dtos []infrastructureNodeServiceDTO
	if err := r.db.Select(context.Background(), &dtos, query, params...); err != nil {
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
