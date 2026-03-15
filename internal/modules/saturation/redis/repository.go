package redis

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const (
	tableMetrics = "observability.metrics"

	colTeamID     = "team_id"
	colTimestamp  = "timestamp"
	colMetricName = "metric_name"
	colValue      = "value"

	attrRedisDB        = "redis.db"
	attrServerAddress  = "server.address"

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
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func instanceFilter(instance string) (string, []any) {
	if instance == "" {
		return "", nil
	}
	return fmt.Sprintf(" AND attributes.'%s'::String = ?", attrServerAddress), []any{instance}
}

func redisAttrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}

func (r *ClickHouseRepository) QueryMetricSeries(teamID int64, startMs, endMs int64, metricName, instance string) ([]MetricPoint, error) {
	instSQL, instArgs := instanceFilter(instance)
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
			%s AS timestamp,
			avg(%s) AS value
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = ?%s
		GROUP BY timestamp
		ORDER BY timestamp ASC
	`, bucket, colValue, tableMetrics, colTeamID, colTimestamp, colMetricName, instSQL)

	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), metricName}, instArgs...)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]MetricPoint, len(rows))
	for i, row := range rows {
		value := dbutil.Float64FromAny(row["value"])
		points[i] = MetricPoint{
			Timestamp: dbutil.StringFromAny(row["timestamp"]),
			Value:     &value,
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) GetCacheHitRate(teamID int64, startMs, endMs int64, instance string) (CacheHitRate, error) {
	instSQL, instArgs := instanceFilter(instance)
	query := fmt.Sprintf(`
		SELECT
			maxIf(%s, %s = '%s') AS hits,
			maxIf(%s, %s = '%s') AS misses
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')%s
	`, colValue, colMetricName, metricRedisKeyspaceHits,
		colValue, colMetricName, metricRedisKeyspaceMisses,
		tableMetrics, colTeamID, colTimestamp, colMetricName,
		metricRedisKeyspaceHits, metricRedisKeyspaceMisses, instSQL)

	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, instArgs...)
	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil {
		return CacheHitRate{}, err
	}

	hits := dbutil.Float64FromAny(row["hits"])
	misses := dbutil.Float64FromAny(row["misses"])
	total := hits + misses
	hitRatePct := 0.0
	if total > 0 {
		hitRatePct = (hits / total) * 100
	}

	return CacheHitRate{
		HitRatePct: hitRatePct,
		Hits:       hits,
		Misses:     misses,
	}, nil
}

func (r *ClickHouseRepository) GetReplicationLag(teamID int64, startMs, endMs int64, instance string) (ReplicationLag, error) {
	instSQL, instArgs := instanceFilter(instance)
	query := fmt.Sprintf(`
		SELECT
			maxIf(%s, %s = '%s') AS replication_offset,
			maxIf(%s, %s = '%s') AS backlog_offset
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')%s
	`, colValue, colMetricName, metricRedisReplicationOffset,
		colValue, colMetricName, metricRedisReplicationBacklogFirst,
		tableMetrics, colTeamID, colTimestamp, colMetricName,
		metricRedisReplicationOffset, metricRedisReplicationBacklogFirst, instSQL)

	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, instArgs...)
	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil {
		return ReplicationLag{}, err
	}

	offset := dbutil.Float64FromAny(row["replication_offset"])
	backlog := dbutil.Float64FromAny(row["backlog_offset"])
	if backlog > 0 && offset >= backlog {
		offset -= backlog
	}

	return ReplicationLag{Offset: offset}, nil
}

func (r *ClickHouseRepository) GetKeyspace(teamID int64, startMs, endMs int64, instance string) ([]KeyspaceRow, error) {
	instSQL, instArgs := instanceFilter(instance)
	query := fmt.Sprintf(`
		SELECT
			coalesce(nullIf(%s, ''), '0') AS redis_db,
			max(%s) AS key_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'%s
		GROUP BY redis_db
		ORDER BY key_count DESC, redis_db ASC
	`, redisAttrString(attrRedisDB), colValue, tableMetrics, colTeamID, colTimestamp, colMetricName, metricRedisDBKeys, instSQL)

	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, instArgs...)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	result := make([]KeyspaceRow, len(rows))
	for i, row := range rows {
		result[i] = KeyspaceRow{
			RedisDB:  dbutil.StringFromAny(row["redis_db"]),
			KeyCount: dbutil.Float64FromAny(row["key_count"]),
		}
	}
	return result, nil
}

func (r *ClickHouseRepository) GetKeyExpiries(teamID int64, startMs, endMs int64, instance string) ([]KeyExpiryRow, error) {
	instSQL, instArgs := instanceFilter(instance)
	query := fmt.Sprintf(`
		SELECT
			coalesce(nullIf(%s, ''), '0') AS redis_db,
			max(%s) AS expiry_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'%s
		GROUP BY redis_db
		ORDER BY expiry_count DESC, redis_db ASC
	`, redisAttrString(attrRedisDB), colValue, tableMetrics, colTeamID, colTimestamp, colMetricName, metricRedisDBExpires, instSQL)

	args := append([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, instArgs...)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	result := make([]KeyExpiryRow, len(rows))
	for i, row := range rows {
		result[i] = KeyExpiryRow{
			RedisDB:     dbutil.StringFromAny(row["redis_db"]),
			ExpiryCount: dbutil.Float64FromAny(row["expiry_count"]),
		}
	}
	return result, nil
}
