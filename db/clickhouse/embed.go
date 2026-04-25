// Package clickhouse exposes the embedded DDL filesystem so the migrator
// in internal/infra/database_chmigrate can apply schema during server boot.
package clickhouse

import "embed"

// FS holds the ordered schema files (00_*, 01_*, …) picked up at compile time.
// Files are applied in lexical order by the migrator.
//
//go:embed *.sql
var FS embed.FS
