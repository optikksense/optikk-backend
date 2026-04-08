# System Architecture: Backend

## Design Goals

Optikk's backend must satisfy two potentially conflicting requirements: **High-Speed Real-time Performance** and **Architectural Extensibility**.

## Strategic Patterns for Agents

### 1. In-Memory to Redis Migration Path
- **Rule**: All stateful services (e.g., live tail subscriptions, temporary aggregation) must use **Interfaces**.
- **Action**: Implement an `In-Memory` provider for speed during development and small-scale deployments.
- **Rule**: Ensure the interface is compatible with Redis-style pub/sub or caching for future migration.

### 2. Channels to Kafka Migration Path
- **Rule**: Use Go channels for ingestion and item streaming by default for simplicity and speed.
- **Action**: Wrap the ingestion pipeline (e.g., `ingestQueue`) in a way that it can be replaced with Kafka or similar message brokers without modifying the OTLP handlers.

### 3. ClickHouse Batched Ingestion
- **Principle**: Optimize for ClickHouse's storage-optimized architecture.
- **Rule**: Always use `streamworkers` and `CHFlusher` patterns for high-throughput observability data.
- **Rule**: Implement logic for retry-backoff in `CHFlusher` to handle momentary ClickHouse connection issues.

### 4. Infrastructure vs Core
- **Rule**: All database-specific code (SQL, ClickHouse-specific types) remains in `repository.go`, `mapper.go`, and `schema.go`.
- **Action**: Services should be infrastructure-agnostic, using interfaces to interact with the database tier.
