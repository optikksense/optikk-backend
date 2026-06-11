// Package mysql exposes the embedded DDL filesystem so the MySQL migrator
// in internal/infra/database can apply schema during server boot.
package mysql

import "embed"

// FS holds compile-time schema files. Re-runs are safe (IF NOT EXISTS).
//
//go:embed *.sql
var FS embed.FS
