package topology

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetTopologyNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]topologyNodeRow, error)
	GetTopologyEdges(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopologyEdge, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
	}
}

func (r *ClickHouseRepository) GetTopologyNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]topologyNodeRow, error) {
	var rows []topologyNodeRow
	err := r.db.Select(ctx, &rows, `
		SELECT service_name,
		       request_count,
		       error_count,
		       avg_latency
		FROM (
			SELECT s.service_name AS service_name,
			       count()                          AS request_count,
			       countIf(`+ErrorCondition()+`)    AS error_count,
			       avg(s.duration_nano / 1000000.0) AS avg_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name
		)
		ORDER BY request_count DESC
	`, baseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetTopologyEdges(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopologyEdge, error) {
	var rows []TopologyEdge
	err := r.db.Select(ctx, &rows, `
		SELECT source,
		       target,
		       call_count,
		       avg_latency,
		       p95_latency_ms,
		       if(call_count > 0, error_count*100.0/call_count, 0) AS error_rate
		FROM (
			SELECT s1.service_name                               AS source,
			       s2.service_name                               AS target,
			       count()                                      AS call_count,
			       countIf(s1.has_error = true OR toUInt16OrZero(s1.response_status_code) >= 400) AS error_count,
			       avg(s1.duration_nano / 1000000.0)            AS avg_latency,
			       quantile(0.95)(s1.duration_nano / 1000000.0) AS p95_latency_ms
			FROM observability.spans s1
			JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id
				AND s2.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s2.timestamp BETWEEN @start AND @end
			WHERE s1.team_id = @teamID AND s1.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s1.kind = 3 AND s1.timestamp BETWEEN @start AND @end
			  AND s1.service_name != s2.service_name
			GROUP BY s1.service_name, s2.service_name
		)
		ORDER BY call_count DESC
		LIMIT `+fmt.Sprintf("%d", MaxEdges),
		baseParams(teamID, startMs, endMs)...)
	return rows, err
}
