// Package mysql exposes the embedded DDL filesystem so the MySQL migrator
// in internal/infra/database can apply schema during server boot.
package mysql

import "embed"

// FS holds the schema file(s) picked up at compile time.
// All statements use CREATE TABLE IF NOT EXISTS, so re-runs are safe.
//
//go:embed *.sql
var FS embed.FS
