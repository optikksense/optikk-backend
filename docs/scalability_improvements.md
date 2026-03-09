# Scaling Audit Report

## Frontend Findings

**1. Chart Resizing Under Load**
* **Location:** `src/shared/components/ui/dashboard/ConfigurableChartCard.tsx`
* **Issue:** Charts using `react-chartjs-2` are placed inside a `flex: 1` div wrapper without bounding constraints. When `Chart.js` is set to `responsive: true` and `maintainAspectRatio: false` in a flex container, it causes an infinite resize loop if tooltips or axis labels increase the chart dimensions slightly. The flexible container expands, telling the chart to expand again.
* **Impact:** High CPU usage, visual jittering, and chart rendering failures under load.
* **Fix:** The div wrapper for the `ChartComponent` needs `minHeight: 0`, `minWidth: 0`, and `position: 'relative'` to constrain the `Chart.js` canvas correctly.

**2. Traces Page Empty Despite Data (Infinite Render Loop)**
* **Location:** `src/features/traces/hooks/useTracesExplorer.ts`
* **Issue:** The React Query hook for fetching traces passes `Date.now()` directly into the `traceQueries.list(...)` options. Because the timestamp is incorporated into the `queryKey`, every React render cycle generates a completely new query key.
* **Impact:** The query cache is perpetually bypassed. React Query immediately falls back to `isLoading: true` and throws away existing data on every render loop, leaving the page completely empty and constantly fetching.
* **Fix:** Extract the `Date.now()` bounds resolution either into a stable memo hook (only recalculating on interval) or move time resolution inside the `queryFn` like `useTimeRangeQuery` does.

**3. Dashboard Auto-Refresh Wiping Out View**
* **Location:** `src/shared/hooks/useComponentDataFetcher.ts`
* **Issue:** Dashboards compute `startMs` and `endMs` when the global `refreshKey` triggers an update (e.g. every 30s). These new timestamps are injected directly into the `useQueries` dependency `queryKey`. When a query key changes in React Query, it treats it as a new distinct request, dropping the old data and showing loaders/empty states while the request completes.
* **Impact:** On every auto-refresh, the entire dashboard flashes empty before re-rendering the updated data.
* **Fix:** Add `placeholderData: keepPreviousData` (from `@tanstack/react-query`) to the query configuration in `useComponentDataFetcher`.

## Backend Findings

**1. Heavy Quantile Calculations for All Endpoints (Unfiltered Subqueries)**
* **Location:** `internal/modules/overview/overview/repository.go` (`GetTopEndpoints`)
* **Issue:** To return the top 100 endpoints, the query computes exact quantiles (`P50`, `P95`, `P99`) for **every** operation in the time period inside a subquery before wrapping it in `ORDER BY request_count DESC LIMIT 100`.
* **Impact:** Computing quantiles across millions of rows for high-cardinality endpoints (before filtering) is extremely memory intensive and scales poorly in ClickHouse.
* **Fix:** Use a subquery to find the `top 100` operations by count first, then `JOIN` or filter the raw spans to compute quantiles only for those 100 operations.

**2. High Cardinality Grouping (Time Series)**
* **Location:** `internal/modules/overview/overview/repository.go` (`GetEndpointTimeSeries`)
* **Issue:** Groups the entire spans table by `time_bucket, service_name, name, http_method` and computes quantiles for each bucket before ordering by `request_count DESC LIMIT 10000`.
* **Impact:** For a service with dynamically generated paths, this grouping will cause memory exhaustion.
* **Fix:** Filter down to the top operations first, or aggregate only the necessary endpoints.

**3. Pagination with OFFSET instead of Keyset**
* **Location:** `internal/modules/spans/store.go` (`GetTraces`)
* **Issue:** Uses `LIMIT ? OFFSET ?`. ClickHouse performs poorly with large offsets because it has to sort all rows up to `offset + limit` before discarding the prefix.
* **Impact:** Browsing older traces (deep pagination) will timeout.
* **Fix:** The frontend needs to transition to `GetTracesKeyset` which uses cursor-based pagination (already implemented in the backend), or enforce a maximum offset.

**4. Synchronous Insert Bottlenecks**
* **Location:** `internal/platform/ingest/ingest.go`
* **Issue:** Background worker flushes were using standard block inserts without `async_insert=1`.
* **Impact:** High concurrent ingestion could stall clickhouse connection threads and overwhelm part mergers when handling highly concurrent telemetry data.
* **Fix:** Enforced `async_insert=1, wait_for_async_insert=1` natively in the batched `INSERT INTO` queries inside the ingest pipeline.
