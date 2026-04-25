// Package chmigrate applies ClickHouse DDL migrations embedded in the binary.
//
// Design: zero external deps beyond the existing clickhouse-go driver. Each
// file in the embed.FS is treated as one migration; its basename is the
// version. A tracking table `<database>.schema_migrations` records applied
// versions. Re-runs are idempotent — already-applied files are skipped.
//
// The schema is additive-only (Phase 5–9 never rewrites earlier files), so
// there is no `Down` operation. Mistakes are corrected by writing a new
// migration with a higher version number.
package chmigrate

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Migrator applies schema migrations from FS to the target ClickHouse
// database. See package doc for semantics.
type Migrator struct {
	// DB is an open clickhouse-go connection with CREATE privileges on
	// Database.
	DB clickhouse.Conn

	// FS contains *.sql files applied in lexical basename order. Typically
	// `db/clickhouse.FS` (the package-level embed).
	FS fs.FS

	// Database is the schema the tracking table lives under — matches the
	// database referenced inside the *.sql files. Defaults to "observability".
	Database string

	// Logger receives one line per applied / skipped file. Optional.
	Logger func(format string, args ...any)
}

// Status describes the apply state for a single migration file.
type Status struct {
	Version string // file basename
	Applied bool
}

const trackingTable = "schema_migrations"

// Up applies every pending migration in lexical order, stopping on first error.
// Safe to call concurrently with other readers but NOT with another Up (see
// package doc).
func (m *Migrator) Up(ctx context.Context) (applied int, skipped int, err error) {
	if err := m.ensureTrackingTable(ctx); err != nil {
		return 0, 0, fmt.Errorf("chmigrate: ensure tracking table: %w", err)
	}
	files, err := m.listFiles()
	if err != nil {
		return 0, 0, err
	}
	appliedSet, err := m.appliedVersions(ctx)
	if err != nil {
		return 0, 0, err
	}

	for _, name := range files {
		if _, ok := appliedSet[name]; ok {
			skipped++
			m.logf("skip  %s (already applied)", name)
			continue
		}
		body, readErr := fs.ReadFile(m.FS, name)
		if readErr != nil {
			return applied, skipped, fmt.Errorf("chmigrate: read %s: %w", name, readErr)
		}
		for i, stmt := range splitStatements(string(body)) {
			if execErr := m.DB.Exec(ctx, stmt); execErr != nil {
				return applied, skipped, fmt.Errorf("chmigrate: %s: statement %d: %w\n--- SQL ---\n%s\n-----------", name, i+1, execErr, stmt)
			}
		}
		if insertErr := m.recordApplied(ctx, name); insertErr != nil {
			return applied, skipped, fmt.Errorf("chmigrate: record %s: %w", name, insertErr)
		}
		applied++
		m.logf("apply %s", name)
	}
	return applied, skipped, nil
}

// Status returns the applied/pending state for every file, preserving the
// apply order. Useful for the `migrate status` subcommand.
func (m *Migrator) Status(ctx context.Context) ([]Status, error) {
	if err := m.ensureTrackingTable(ctx); err != nil {
		return nil, err
	}
	files, err := m.listFiles()
	if err != nil {
		return nil, err
	}
	appliedSet, err := m.appliedVersions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Status, len(files))
	for i, name := range files {
		_, ok := appliedSet[name]
		out[i] = Status{Version: name, Applied: ok}
	}
	return out, nil
}

func (m *Migrator) ensureTrackingTable(ctx context.Context) error {
	if m.Database == "" {
		m.Database = "observability"
	}
	// The `observability` database itself is created by the first migration
	// (`00_database.sql`). When a fresh cluster runs Up for the first time,
	// the tracking table must live in the default db until that migration
	// lands — but then every *subsequent* run expects it under observability.
	//
	// Simplest fix: create the database first (idempotent), then the table.
	if err := m.DB.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+m.Database); err != nil {
		return err
	}
	return m.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
		    version    String,
		    applied_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY version
	`, m.Database, trackingTable))
}

func (m *Migrator) listFiles() ([]string, error) {
	var names []string
	err := fs.WalkDir(m.FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".sql") {
			names = append(names, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("chmigrate: walk: %w", err)
	}
	sort.Strings(names)
	return names, nil
}

func (m *Migrator) appliedVersions(ctx context.Context) (map[string]struct{}, error) {
	rows, err := m.DB.Query(ctx, fmt.Sprintf("SELECT version FROM %s.%s", m.Database, trackingTable))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	set := map[string]struct{}{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		set[v] = struct{}{}
	}
	return set, rows.Err()
}

func (m *Migrator) recordApplied(ctx context.Context, version string) error {
	return m.DB.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s.%s (version) VALUES (?)", m.Database, trackingTable),
		version,
	)
}

func (m *Migrator) logf(format string, args ...any) {
	if m.Logger == nil {
		return
	}
	m.Logger(format, args...)
}

// splitStatements breaks a multi-statement SQL blob into individual
// statements. It strips `-- line comments`, ignores whitespace-only
// fragments, and splits on `;` that is not inside a single-quoted string.
//
// Our DDL files don't embed semicolons inside string literals (verified at
// commit time — see plan risks). If a future migration does, switch to a
// proper tokenizer.
func splitStatements(sql string) []string {
	var out []string
	var buf strings.Builder
	inSingle := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		// Strip `-- line comment` to end-of-line (but not inside strings).
		if !inSingle && c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if c == '\'' {
			// Handle escaped single-quote (`''`) inside a literal.
			if inSingle && i+1 < len(sql) && sql[i+1] == '\'' {
				buf.WriteByte(c)
				buf.WriteByte(sql[i+1])
				i++
				continue
			}
			inSingle = !inSingle
			buf.WriteByte(c)
			continue
		}
		if c == ';' && !inSingle {
			stmt := strings.TrimSpace(buf.String())
			if stmt != "" {
				out = append(out, stmt)
			}
			buf.Reset()
			continue
		}
		buf.WriteByte(c)
	}
	if tail := strings.TrimSpace(buf.String()); tail != "" {
		out = append(out, tail)
	}
	return out
}
