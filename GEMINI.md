# Gemini System Instructions

Welcome, Gemini agent!

When working in `optikk-backend`, always align with these principles to ensure success and maintain our "Staff-level" engineering culture:

## 1. Type Safety is Paramount
We strictly prohibit fragile design patterns such as positional `[]any` slicing or `interface{}` wrapper hacks where static typing is sufficient. Use explicit, domain-specific structs (e.g., `LogRow`, `SpanRow`) heavily coupled with clear SQL tags like `ch:"column_name"`. Generic constructs (e.g. `Dispatcher[T]`) should be used over interface wrappers. Reference **ADR 002 (Typed Ingestion Pipeline)**.

## 2. Modularity via 6-file Pattern
Any time you create or modify an API feature module, respect the strict 6-file pattern: `handler.go`, `service.go`, `repository.go`, `module.go`, `dto.go`, and `models.go`. Do NOT fragment these logical components into multiple subfiles. Reference **ADR 001**. 

## 3. No Dummy Values for Business Attributes
Do not invent or fill "dummy" fallback values (like `"default"`, `"unknown"`, or `-1`) for telemetry business attributes if those values are not provided by the payload. Let empty fields remain empty strings (`""`) or zero values (`0`). **Exception:** Core ingestion timestamps MUST defensively fallback to `time.Now()` if missing to ensure ClickHouse time partitions and buckets process the row successfully.

## 4. Eliminate Redundancy
Redundant duplicate files, functions, or variables must be removed immediately to maintain codebase integrity and prevent synchronization issues.

## 5. Review CODEBASE_INDEX and .cursor/rules
You must align with `.cursor/rules/optik-backend.mdc` and `CODEBASE_INDEX.md` prior to any major infrastructural or architectural change. Always verify that your actions keep these indexes up to date.

## 6. Execution Workflow
Always provide a detailed plan before executing breaking changes. Leverage artifacts and ask clarifying questions if anything feels ambiguous. 

Thanks for keeping Optikk stable and excellent!
