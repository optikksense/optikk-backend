---
description: Maintain the CODEBASE_INDEX.md when architecture or modules changes in optikk-backend.
---

# Workflow: Update Codebase Index

You must run this workflow after adding new HTTP modules, ingestion paths, or materially changing overview/data APIs documented in the index.

1. **Scan the changes**: Identify new domains in `internal/app/server/modules_manifest.go` or new endpoints in `internal/modules/.../handler.go`.
2. **Review [CODEBASE_INDEX.md](../../CODEBASE_INDEX.md)**: Find the relevant section (Module packages, Ingestion, or Cross-repo map).
3. **Draft the Update**: Ensure the new entry follows the existing table format.
| Domain | Packages (representative) |
|--------|---------------------------|
4. **Apply Changes**: Update the file to maintain truth in documentation.
5. **Cross-Reference**: If the change involves the frontend, ensure the **Backend ↔ frontend map** is updated in both repositories.
