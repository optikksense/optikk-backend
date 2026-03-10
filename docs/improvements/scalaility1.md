Scalability Audit — Bottleneck Report
Context
Audit of the Go observability backend to identify scalability bottlenecks under production load. Excludes already-known items: Kafka integration, Redis rate limiter, SSE chart connections.

CRITICAL — Fix First
1. No HTTP Server Timeouts
File: internal/platform/server/app.go (lines 476-486) Both HTTP servers (mainSrv, OTLP HTTP) have zero ReadTimeout, WriteTimeout, IdleTimeout. Slow/malicious clients hold goroutines forever → memory exhaustion, slowloris vulnerability.

2. gRPC Missing Concurrency Limits
File: internal/platform/server/app.go (lines 493-495) Only MaxRecvMsgSize is set. Missing MaxConcurrentStreams, KeepaliveParams, ConnectionTimeout. Burst traffic spawns unlimited goroutines.

3. Zero Query Result Caching
No caching layer exists for expensive ClickHouse aggregations (service map, topology, RED metrics, error rates). Every dashboard load re-executes identical full table scans. A 30-60s Redis/in-memory cache on hot endpoints would dramatically reduce CH load.

4. OFFSET Pagination on Spans
File: internal/modules/spans/store.go (lines 226-284) GetTraces uses LIMIT ? OFFSET ? — scanning all rows up to the offset. Page 1000 scans 100K rows. Keyset pagination exists (GetTracesKeyset) but isn't the default.

HIGH — Production Blockers
5. JOIN Fan-Out in Service Map & Topology
Files: internal/modules/services/servicemap/repository.go (lines 30-74), internal/modules/services/topology/repository.go (lines 44-68) Self-join on spans table (s1 JOIN s2 ON trace_id + parent_span_id) can produce cartesian explosion before GROUP BY. Filters applied after join, not before.

6. Trace Detail In-Memory Tree (10K Spans)
File: internal/modules/spans/tracedetail/repository.go

GetCriticalPath (lines 176-292): Loads all spans (LIMIT 10000) into map[string]*node, builds tree, DFS traversal
GetTraceSpans (lines 286-329): Same 10K span load + JSON attribute parsing
Large traces cause GC pauses; deep recursion risks stack issues
7. Multiple Full Scans Per Request
Log facets (internal/modules/log/repository.go lines 548-572): 3 separate queries (severity, service, host) each re-scanning same data
Log stats (lines 574-639): UNION ALL of 5 branches with identical WHERE, args repeated 5×
Trace logs (lines 443-543): Up to 4 speculative queries for one request
Trace detail span events (lines 55-135): 2 separate queries on same trace_id
8. Auth Layer Lock Contention
API key cache (internal/platform/otlp/auth/auth.go lines 18-72): Single sync.RWMutex on global map. All ingest handlers (HTTP + gRPC) contend on this lock. Cache is also unbounded (no eviction).
Token blacklist (internal/platform/auth/blacklist.go): SHA256 hash + RWMutex check on every authenticated request. No negative caching ("not revoked" result not cached).
JWT parsing (internal/platform/middleware/middleware.go line 175): Full HMAC-SHA256 signature verification on every request. No claims cache.
9. ByteTracker Mutex Contention
File: internal/platform/ingest/tracker.go (line 37) Every ingested row calls Track() which acquires a mutex. At 10K+ rows/sec across goroutines, this serializes accounting.

MEDIUM — Performance Degradation Under Load
10. Single Ingest Consumer per Queue
File: internal/platform/ingest/ingest.go (line 118) One worker goroutine consumes the ring buffer. Flush concurrency is capped at 4 (hardcoded, line 81). Bottleneck at ~10-30K spans/sec.

11. Sequence Barrier Incomplete Batches
File: internal/platform/ingest/ingest.go (lines 130-132) consume() stops if a producer claimed a slot but hasn't stored yet (v == nil). Causes latency spikes — stragglers wait for next flush cycle.

12. Circuit Breaker Thundering Herd
File: internal/platform/circuit_breaker/breaker.go After 5 failures → open for 30s → half-open. No jitter on retry. All queued requests retry simultaneously.

13. ClickHouse SETTINGS Too Permissive
File: internal/database/clickhouse.go (lines 141-147) max_rows_to_read=500M applied to ALL SELECT queries. No max_result_rows setting. A single bad query can scan 500M rows.

14. Connection Pool May Be Undersized
File: internal/database/clickhouse.go (lines 59-61) 50 max connections shared across all modules. With 10+ concurrent dashboard users each triggering multiple queries, pool exhaustion causes request queueing.

15. OTLP Body Parsing — Full Buffering + Temp Allocations
File: internal/platform/otlp/handler.go (line 46), mapper.go (lines 222-325)

50MB max body fully buffered in memory before JSON parsing
Per-span: 2 map allocations (spanMap + mergedMap). 10K-span batch = 20K allocations → GC pressure
16. Unbounded Attribute Filters
Files: internal/modules/spans/store.go (lines 79-94), internal/modules/log/repository.go (lines 118-132) No cap on number of AttributeFilter conditions. Each adds a map lookup in ClickHouse. Client can send 100 filters → extremely slow query.

17. No MySQL Circuit Breaker
File: internal/database/mysql.go Unlike ClickHouse, MySQL queries have no circuit breaker or failure protection. Cascading failures possible if MySQL becomes slow.

18. Summary Stats Duplicate Scan
File: internal/modules/spans/store.go (lines 146-168) Stats query (p50, p95, p99) runs independently from paginated data query with identical WHERE. Same data scanned twice.

LOW — Optimization Opportunities
19. ClickHouse Quantiles on Full Datasets
Heavy use of quantile(0.95)(duration_ms) without sampling. For non-critical dashboards, quantileTiming() or quantileApprox() would be cheaper.

20. PREWHERE Regex Fragility
File: internal/database/clickhouse.go (lines 128-139) Regex only matches if conditions appear in exact order (team_id = ? AND ts_bucket_start BETWEEN). Different filter ordering breaks the optimization silently.

21. In-Process Rate Limiter Single Mutex
File: internal/platform/middleware/ratelimit.go (lines 47-73) Global mutex on bucket map. Sharding (16-32 buckets) would reduce contention.

22. gRPC Graceful Shutdown Blocks Indefinitely
File: internal/platform/server/app.go GracefulStop() blocks if streams don't close. No timeout on Phase 1 (queue drain) either.