Scalability Audit: Datadog-Level Readiness
CRITICAL — Blocks any serious scale
Issue	Current	Datadog-Level	Fix
Monolith	Single process serves ingest + queries + alerts + WebSocket	Separate ingest, query, alert, and real-time tiers	Split into 4 deployable binaries sharing the same module code
Ingest buffer drops silently	10K channel buffer, select/default drops with a log every 100	Backpressure to clients (gRPC ResourceExhausted)	Return error to sender when queue is full instead of silent drop
Single persistence writer	1 goroutine writes logs+spans+metrics in a shared select loop	Per-type writer pools (N goroutines per type)	Fan out to 3-8 parallel ClickHouse batch writers
gRPC MaxConcurrentStreams=100	Hardcoded in app.go:128	10K+ concurrent streams behind LB	Make configurable, default to 1000+
ClickHouse pool = 15 conns	defaultCHMaxOpenConns=15 in clickhouse.go:31	100+ with separate read/write pools	Split into ingest pool (dedicated) + query pool
Alert evaluator is single-threaded	1 goroutine, 30s tick, evaluates all rules serially	Worker pool with per-rule timeout	Parallelize with N workers, bounded concurrency
LiveTail hub is in-memory only	LocalHub with sync.RWMutex, no Redis option	Redis pub-sub with per-team sharding	Implement Redis provider behind existing Hub interface
HIGH — Causes degradation under load
Issue	Current	Impact	Fix
Cache hit rate ~5%	Key includes full RawQuery (timestamps change every request), 30s TTL	Every dashboard refresh = cache miss	Bucket timestamps to nearest minute in cache key, increase TTL to 60-120s
Logs use OFFSET pagination	logs/search/repository.go — LIMIT ? OFFSET ? + separate COUNT(*)	O(n) at deep pages, doubles query cost	Keyset cursor pagination (traces already have this)
Unbounded GROUP BY	Facet queries in logs/explorer/repository.go — 5 UNION ALL with no LIMIT per group	High-cardinality fields explode result sets	Add LIMIT 100 per facet group
No per-tenant isolation	Shared pools, queues, evaluator; one tenant can starve all	Noisy neighbor problem	Per-tenant ingestion quotas, query concurrency semaphores
Silent result truncation	max_result_rows=100000 with result_overflow_mode='break'	Clients get partial data, don't know it's truncated	Add was_truncated field to response
Alert dispatch buffer = 512	dispatcher.go — drops silently when full	Alert storms lose notifications	Increase buffer, add metrics, consider persistent queue
Alert idempotency map unbounded	seenKeys map[string]struct{} — never evicted	Memory leak proportional to alert churn	Add TTL-based eviction or use ttlcache
MEDIUM — Technical debt that compounds
Issue	Current	Fix
GC pressure in mappers	Per-span/log: 3-5 map allocations, no pooling	sync.Pool for common map sizes
Byte tracker silent loss	MySQL flush fails → counters lost, no retry	Add retry with backoff
Auth cache per-instance	5-min TTL, each instance has own cache	Acceptable for now; add Redis-backed cache at 10+ instances
QueryMaps no pre-alloc	make([]map[string]any, 0) grows via append	Pre-allocate with make(..., 0, limit)
NaN normalization per row	Applied to every cell in every query response	Move to ClickHouse-side (if(isNaN(x), 0, x))
What's already good
PREWHERE optimization — all span/log queries use ts_bucket_start for partition pruning
Keyset pagination for traces — efficient cursor-based paging
Auth caching — TTL-based ttlcache avoids DB lookups on every ingest request
Byte tracking — lock-free atomic.AddInt64 via sync.Map, efficient at high throughput
Redis sessions — production requires Redis, enabling horizontal API scaling
Graceful shutdown — oklog/run coordinates HTTP + gRPC + background workers
Parameterized queries — no SQL injection risk in query compilation
Recommended priority for Datadog-level readiness
Split ingest from query — separate binaries, separate CH connection pools
Parallel ingest writers — 3 goroutines (one per type) with configurable concurrency
Backpressure instead of drops — return gRPC error when queue is full
Increase all pool/buffer defaults — CH conns 15→100, gRPC streams 100→1000, dispatch buffer 10K→100K
Redis pub-sub for LiveTail — implement the provider interface that already exists
Parallel alert evaluation — worker pool with per-rule timeout
Fix cache key strategy — bucket timestamps, increase TTL
Keyset pagination for logs — migrate from OFFSET