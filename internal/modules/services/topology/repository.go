package topology

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
)

const (
	spansRollupPrefix   = "observability.spans_rollup"
	topologyRollupPrefix = "observability.spans_topology_rollup"
)

// Repository runs ClickHouse queries that power the runtime service topology.
type Repository interface {
	GetNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]nodeAggRow, error)
	GetEdges(ctx context.Context, teamID int64, startMs, endMs int64) ([]edgeAggRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func topologyParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// GetNodes aggregates per-service RED metrics from the Phase-5 spans rollup.
// Rollup stores root-span state (server/consumer) per (service, operation,
// endpoint, method) — topology wants the service-level rollup, so we sum /
// merge across the operation-level rows.
func (r *ClickHouseRepository) GetNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]nodeAggRow, error) {
	table, _ := rollup.TierTableFor(spansRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name                                                            AS service_name,
		       toInt64(sumMerge(request_count))                                        AS request_count,
		       toInt64(sumMerge(error_count))                                          AS error_count,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1     AS p50_ms,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2     AS p95_ms,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3     AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service_name != ''
		GROUP BY service_name
	`, table)

	var rows []nodeAggRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, topologyParams(teamID, startMs, endMs)...)
	return rows, err
}

// GetEdges reads the Phase-7 `spans_topology_rollup` cascade. The MV keys on
// (client_service, server_service) — edges are extracted single-pass from
// CLIENT-kind spans using the mat_peer_service attribute. Bypasses the old
// span self-join approach entirely.
func (r *ClickHouseRepository) GetEdges(ctx context.Context, teamID int64, startMs, endMs int64) ([]edgeAggRow, error) {
	table, _ := rollup.TierTableFor(topologyRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT client_service                                                         AS source,
		       server_service                                                         AS target,
		       toInt64(sumMerge(request_count))                                       AS call_count,
		       toInt64(sumMerge(error_count))                                         AS error_count,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1    AS p50_ms,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2    AS p95_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND client_service != ''
		  AND server_service != ''
		  AND client_service != server_service
		GROUP BY source, target
	`, table)

	var rows []edgeAggRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, topologyParams(teamID, startMs, endMs)...)
	return rows, err
}
