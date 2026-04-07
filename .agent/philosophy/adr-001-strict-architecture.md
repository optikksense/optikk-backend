# ADR-001: Strict Backend Architecture

## Status
Accepted (Standardized June 2026)

## Context
The `optikk-backend` previously suffered from fragmented repository and handler implementations. To improve maintainability and strategic alignment, we adopted a strict modular architecture in Go.

## Decisions

### 1. The 6-File Module Pattern
- **Decision**: Every feature/module in `internal/modules` MUST adhere to a strict 6-file structure.
- **Files**:
  - `handlers.go`: Final Gin/HTTP entry points.
  - `service.go`: Business logic, coordination, and validation.
  - `repository.go`: Database interactions (MariaDB/ClickHouse).
  - `mapper.go`: Conversions between DTOs and Database entities.
  - `schema.go`: Database table definitions.
  - `types.go`: Shared internal data structures.
- **Agent Rule**: Consolidated fragmented repositories into a single `repository.go` per module. Never use separate implementation files (e.g., `repository_impl.go`).

### 2. Infrastructure Abstraction
- **Decision**: Pass dependencies (repositories, clients) via interfaces to the service layer.
- **Rationale**: Allows for future extensibility (e.g., swapping a direct DB call for a cache or a different message broker).
- **Agent Rule**: Services should never depend on a concrete repository implementation; always use interfaces.

### 3. ClickHouse Performance
- **Decision**: All ClickHouse ingestion must be batched (Flush).
- **Agent Rule**: Use `streamworkers` and `CHFlusher` patterns for high-throughput observability data.
- **Agent Rule**: Explicitly map `time.Time` to `UInt64` (nanoseconds) or `DateTime64` in `mapper.go` to avoid conversion errors.

## Consequences
- **Positive**: Clear boundaries of responsibility; simplified debugging; predictable code placement.
- **Negative**: Adds slight boilerplate for very simple features.
