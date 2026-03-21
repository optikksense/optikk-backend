package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const (
	tableMetrics = "observability.metrics"

	colTeamID     = "team_id"
	colTimestamp  = "timestamp"
	colMetricName = "metric_name"
	colValue      = "value"

	attrRedisDB       = "redis.db"
	attrServerAddress = "server.address"

	metricRedisKeyspaceHits            = "redis.keyspace.hits"
	metricRedisKeyspaceMisses          = "redis.keyspace.misses"
	metricRedisClientsConnected        = "redis.clients.connected"
	metricRedisMemoryUsed              = "redis.memory.used"
	metricRedisMemoryFragmentation     = "redis.memory.fragmentation_ratio"
	metricRedisCommandsProcessed       = "redis.commands.processed"
	metricRedisKeysEvicted             = "redis.keys.evicted"
	metricRedisDBKeys                  = "redis.db.keys"
	metricRedisDBExpires               = "redis.db.expires"
	metricRedisReplicationOffset       = "redis.replication.offset"
	metricRedisReplicationBacklogFirst = "redis.replication.backlog_first_byte_offset"
)

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// baseParams returns named ClickHouse parameters for teamID + time range.
func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func instanceFilter(instance string) (string, []any) {
	if instance == "" {
		return "", nil
	}
	return fmt.Sprintf(" AND attributes.'%s'::String = @instance", attrServerAddress), []any{
		clickhouse.Named("instance", instance),
	}
}

func redisAttrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}

func (r *ClickHouseRepository) QueryMetricSeries(teamID int64, startMs, endMs int64, metricName, instance string) ([]metricPointDTO, error) {
	instSQL, instArgs := instanceFilter(instance)
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
			%s AS time_bucket,
			avg(%s) AS value
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = @metricName%s
		GROUP BY time_bucket
		ORDER BY time_bucket ASC
	`, bucket, colValue, tableMetrics, colTeamID, colTimestamp, colMetricName, instSQL)

	args := append(baseParams(teamID, startMs, endMs), clickhouse.Named("metricName", metricName))
	args = append(args, instArgs...)
	var points []metricPointDTO
	return points, r.db.Select(context.Background(), &points, query, args...)
}

func (r *ClickHouseRepository) GetCacheHitRate(teamID int64, startMs, endMs int64, instance string) (cacheHitRateDTO, error) {
	instSQL, instArgs := instanceFilter(instance)
	query := fmt.Sprintf(`
		SELECT
			maxIf(%s, %s = '%s') AS hits,
			maxIf(%s, %s = '%s') AS misses
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')%s
	`, colValue, colMetricName, metricRedisKeyspaceHits,
		colValue, colMetricName, metricRedisKeyspaceMisses,
		tableMetrics, colTeamID, colTimestamp, colMetricName,
		metricRedisKeyspaceHits, metricRedisKeyspaceMisses, instSQL)

	args := append(baseParams(teamID, startMs, endMs), instArgs...)
	var row cacheHitRateRow
	if err := r.db.QueryRow(context.Background(), &row, query, args...); err != nil {
		return cacheHitRateDTO{}, err
	}
	total := row.Hits + row.Misses
	hitRatePct := 0.0
	if total > 0 {
		hitRatePct = (row.Hits / total) * 100
	}
	return cacheHitRateDTO{HitRatePct: hitRatePct, Hits: row.Hits, Misses: row.Misses}, nil
}

func (r *ClickHouseRepository) GetReplicationLag(teamID int64, startMs, endMs int64, instance string) (replicationLagDTO, error) {
	instSQL, instArgs := instanceFilter(instance)
	query := fmt.Sprintf(`
		SELECT
			maxIf(%s, %s = '%s') AS replication_offset,
			maxIf(%s, %s = '%s') AS backlog_offset
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s')%s
	`, colValue, colMetricName, metricRedisReplicationOffset,
		colValue, colMetricName, metricRedisReplicationBacklogFirst,
		tableMetrics, colTeamID, colTimestamp, colMetricName,
		metricRedisReplicationOffset, metricRedisReplicationBacklogFirst, instSQL)

	args := append(baseParams(teamID, startMs, endMs), instArgs...)
	var row replicationLagRow
	if err := r.db.QueryRow(context.Background(), &row, query, args...); err != nil {
		return replicationLagDTO{}, err
	}
	offset := row.ReplicationOffset
	if row.BacklogOffset > 0 && offset >= row.BacklogOffset {
		offset -= row.BacklogOffset
	}
	return replicationLagDTO{Offset: offset}, nil
}

func (r *ClickHouseRepository) GetKeyspace(teamID int64, startMs, endMs int64, instance string) ([]keyspaceRowDTO, error) {
	instSQL, instArgs := instanceFilter(instance)
	query := fmt.Sprintf(`
		SELECT
			coalesce(nullIf(%s, ''), '0') AS redis_db,
			max(%s) AS key_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'%s
		GROUP BY redis_db
		ORDER BY key_count DESC, redis_db ASC
	`, redisAttrString(attrRedisDB), colValue, tableMetrics, colTeamID, colTimestamp, colMetricName, metricRedisDBKeys, instSQL)

	args := append(baseParams(teamID, startMs, endMs), instArgs...)
	var rows []keyspaceRowDTO
	return rows, r.db.Select(context.Background(), &rows, query, args...)
}

func (r *ClickHouseRepository) GetKeyExpiries(teamID int64, startMs, endMs int64, instance string) ([]keyExpiryRowDTO, error) {
	instSQL, instArgs := instanceFilter(instance)
	query := fmt.Sprintf(`
		SELECT
			coalesce(nullIf(%s, ''), '0') AS redis_db,
			max(%s) AS expiry_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'%s
		GROUP BY redis_db
		ORDER BY expiry_count DESC, redis_db ASC
	`, redisAttrString(attrRedisDB), colValue, tableMetrics, colTeamID, colTimestamp, colMetricName, metricRedisDBExpires, instSQL)

	args := append(baseParams(teamID, startMs, endMs), instArgs...)
	var rows []keyExpiryRowDTO
	return rows, r.db.Select(context.Background(), &rows, query, args...)
}
