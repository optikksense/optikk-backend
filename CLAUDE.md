# CLAUDE.md

## Commands

- **Build server**: `make build` or `go build -v ./cmd/server`
- **Run server**: `make run` or `go run ./cmd/server`
- **Run tests**: `go test ./...`
- **Format code**: `make fmt` or `gofmt -w .`
- **Vet code**: `make vet` or `go vet ./...`
- **Generate Protobuf**: `make proto`

## Guidelines & Invariants

### 1. Think Before Coding
- State assumptions explicitly. If uncertain, ask.
- Present multiple interpretations; do not pick silently.
- Push back or suggest simpler approaches if warranted.

### 2. Simplicity First
- Implement minimum code that solves the problem. No speculative features/abstractions.
- Keep functions and files focused; rewrite overly complex code.

### 3. Surgical Changes
- Touch only what is necessary. Do not refactor adjacent unbroken code.
- Match existing style. Keep track of unused variables/imports your changes created and clean them up.

### 4. Goal-Driven Execution
- Transform tasks into concrete, verifiable success criteria (e.g., reproduction test case).
- Use a step-by-step plan and verify each step.

### 5. Engineering Principles
- **SRP & OCP**: Each file/function owns a single responsibility. Open for extension, closed for modification.
- **DRY**: Represent knowledge authoritatively in a single place.
- **LSP, ISP, DIP**: Depend on abstractions, avoid fat interfaces, substitute types safely.

### 6. Commenting Rules
- Max 2 lines for multiline comments.
- Single line comment on top of function/class.
- Max 80 characters per line.

### 7. Project Specific
- **Repositories**: Contain only SQL queries. All logic must reside in the service layer.
- **Mind-map invariants**:
  - One shape per layer: every module is `module/handler/service/repository/dto/models`.
  - One home per concept: shared CH arg builders live in `internal/shared/chargs`; range-query handler wrapper is `httputil.HandleRangeQuery`; infra OTel constants live in `infraconsts`. Never re-declare locally.
  - One policy per pattern: `Repository`/`Service` are concrete structs. Interfaces only at points of real polymorphism — never for a single implementation.
  - Dependencies point one way: handler → service → repository.
