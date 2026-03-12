# Understanding `ts_bucket_start` in ClickHouse

## Overview
The `ts_bucket_start` column is a critical performance optimization strategy used across our high-volume observability tables (like `spans` and `logs`) in ClickHouse.

At a high level, it calculates a coarsely granular, truncated time bucket (such as a 5-minute window for spans or a 1-day window for logs) from every event's raw timestamp during ingestion.

This column serves two fundamental purposes:
1. **Storing:** It groups temporally adjacent data tightly together on disk.
2. **Querying (Partition Pruning):** It prevents ClickHouse from scanning irrelevant data granules during time-series queries.

---

## 1. How It Helps in Storing Data

ClickHouse is an analytical database built around the `MergeTree` family of engines. Data is written in parts, and background processes merge these parts over time. 

The most important configuration for a `MergeTree` table is its **`ORDER BY`** (sorting key) clause, which dictates how data is physically laid out on disk. 

For example, looking at the `observability.spans` table schema:
```sql
ORDER BY (team_id, ts_bucket_start, service_name, name, timestamp)
```

By placing `ts_bucket_start` immediately after `team_id`, data is clustered first by tenant and then into discrete time buckets. 
- A 5-minute `ts_bucket_start` (300 seconds) forces all spans for a specific team within that exact 5-minute window to be sorted contiguously alongside each other.
- Because `service_name` and `name` naturally have low cardinality within a short 5-minute window, ClickHouse achieves **massive compression ratios**. The storage engine simply compresses identical string blocks together.
- If we didn't use `ts_bucket_start` and simply ordered by `timestamp` first, events would be scattered chronologically down to the millisecond, completely destroying the ability to compress repetitive `service_name` and `name` values efficiently.

---

## 2. How It Helps in Querying Data (Partition Pruning)

When running analytical dashboards, queries always specify a time block:
```sql
WHERE timestamp BETWEEN '2023-10-01 10:04:00' AND '2023-10-01 10:45:00'
```
Because the table is **not** physically sorted primarily by `timestamp` (remember, it is sorted by `team_id`, then `ts_bucket_start`, then `service_name`), finding all rows for that exact timestamp block would normally require scanning millions of rows across the entire team's dataset.

This is where `ts_bucket_start` shines as a **Primary Key indexing hero**. 

During a query:
1. The backend determines the bounding box of `ts_bucket_start` for the desired time window.
2. The query is rewritten (often via `PREWHERE`) to use the bucket:
   ```sql
   PREWHERE team_id = 1 
     AND ts_bucket_start BETWEEN 1696154400 AND 1696156800 
   WHERE timestamp BETWEEN <precise_ns_start> AND <precise_ns_end>
   ```
3. Since `ts_bucket_start` is the second column in the `ORDER BY` clause, ClickHouse can use its dense sparse index to jump **directly** to the granules on disk that contain those 5-minute or 1-day buckets.
4. It bypasses scanning any granules outside those coarse bounds completely. The precise `timestamp` filter in the `WHERE` clause then filters the remaining small fraction of rows down to the exact nanosecond.

### Without `ts_bucket_start`:
ClickHouse would have to decompress and evaluate the `timestamp` column for every single row belonging to `team_id = 1`, resulting in high CPU usage, vast disk I/O, and drastically slower dashboard load times.

---

## Summary of Granularities
The optimization boundaries differ depending on the volume and typical query shape of the signal:
- **Spans:** Truncated to a 5-minute boundary (`300` seconds).
- **Logs:** Truncated to a 1-day boundary (`86400` seconds). 
