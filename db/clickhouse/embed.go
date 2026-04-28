// Package clickhouse exposes the embedded DDL filesystem so the Migrator
// in internal/infra/database can apply schema during server boot.
package clickhouse

import "embed"

// FS holds the ordered schema files (00_*, 01_*, …) picked up at compile time.
// Files are applied in lexical order by the migrator.
//
//go:embed *.sql
var FS embed.FS
