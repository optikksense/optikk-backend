package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "observability",
			Username: "default",
			Password: "clickhouse123",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Drop and recreate table with exact prod schema
	err = conn.Exec(context.Background(), `DROP TABLE IF EXISTS observability.logs`)
	if err != nil {
		log.Fatal(err)
	}

	schema := `CREATE TABLE IF NOT EXISTS observability.logs (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket_start      UInt32 CODEC(Delta(4), LZ4),
    timestamp            UInt64 CODEC(DoubleDelta, LZ4),
    observed_timestamp   UInt64 CODEC(DoubleDelta, LZ4),
    id                   String CODEC(ZSTD(1)),
    trace_id             String CODEC(ZSTD(1)),
    span_id              String CODEC(ZSTD(1)),
    trace_flags          UInt32 DEFAULT 0,
    severity_text        LowCardinality(String) CODEC(ZSTD(1)),
    severity_number      UInt8 DEFAULT 0,
    body                 String CODEC(ZSTD(2)),
    attributes_string    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    attributes_number    Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    attributes_bool      Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    resource             JSON(max_dynamic_paths=100) CODEC(ZSTD(1)),
    resource_fingerprint String CODEC(ZSTD(1)),
    scope_name           String CODEC(ZSTD(1)),
    scope_version        String CODEC(ZSTD(1)),
    scope_string         Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    service              LowCardinality(String) MATERIALIZED resource.` + "`service.name`" + `::String,
    host                 LowCardinality(String) MATERIALIZED resource.` + "`host.name`" + `::String,
    pod                  LowCardinality(String) MATERIALIZED resource.` + "`k8s.pod.name`" + `::String,
    container            LowCardinality(String) MATERIALIZED resource.` + "`k8s.container.name`" + `::String,
    environment          LowCardinality(String) MATERIALIZED resource.` + "`deployment.environment`" + `::String
	) ENGINE = MergeTree()
	PARTITION BY toYYYYMM(toDateTime(ts_bucket_start))
	ORDER BY (team_id, ts_bucket_start, service, timestamp)`

	err = conn.Exec(context.Background(), schema)
	if err != nil {
		log.Fatal("Schema error: ", err)
	}

	// Query with real values
	startMs := int64(1773162542784)
	endMs := int64(1773173342784)
	startNs := uint64(startMs) * 1_000_000
	endNs := uint64(endMs) * 1_000_000

	startBucket := uint32((startMs / 1000) / 86400 * 86400)
	endBucket := uint32((endMs / 1000) / 86400 * 86400)

	query := `SELECT id, timestamp, observed_timestamp, severity_text, severity_number,
	body, trace_id, span_id, trace_flags,
	service, host, pod, container, environment,
	attributes_string, attributes_number, attributes_bool,
	scope_name, scope_version FROM logs WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp DESC, id DESC LIMIT ?`

	args := []any{uint32(1), startBucket, endBucket, startNs, endNs, 20}

	rows, err := conn.Query(context.Background(), query, args...)
	if err != nil {
		log.Fatal("Query error: ", err)
	}
	defer rows.Close()

	fmt.Println("Success")
}
