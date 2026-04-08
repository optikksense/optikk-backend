# Vision & Extensibility: Backend

## Core Mission
Optikk aims to provide a **Developer-First, Datadog-style Observability** platform with a priority on **LLM Observability**.

## Strategic Pillars for Agents

### 1. Extensible Architecture
- **Principle**: The system must be built so that infrastructure components can be swapped without changing the core business logic.
- **Rule**: Implement all infrastructure as **Swappable Providers**. 
- **Example**: An in-memory cache should be easily replaceable with Redis by updating the `provider.go` or `factory.go`, not by rewriting the service layer.

### 2. High-Speed Reliability
- **Principle**: Millisecond response times for small-to-medium clusters are prioritized over global multi-region scale for now.
- **Rule**: Optimize for **latency first**. Use efficient Go concurrency (channels, workers) for real-time streaming (Live Tail).

### 3. LLM Observability Priority
- **Principle**: Telemetry for LLM integrations (tokens, costs, span attributes) is the top product priority.
- **Rule**: Prioritize LLM-specific data models in `schema.go` and `mapper.go`.

### 4. Data vs Architecture
- **Rule**: While system architecture is the current priority over absolute durability, **data integrity** for traces and logs is still critical for developer trust.
- **Rule**: Avoid "silent failures" in ClickHouse/MariaDB ingestion. Always log ingestion errors with structured metadata.
