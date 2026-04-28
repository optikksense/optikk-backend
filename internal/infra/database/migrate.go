package database

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Migrator applies ClickHouse DDL migrations from FS to the target database.
//
// Each file in FS is one migration; its basename is the version. A tracking
// table `<Database>.schema_migrations` records applied versions. Re-runs are
// idempotent — already-applied files are skipped. The schema is
// additive-only; mistakes are corrected by writing a new migration with a
// higher version number, never by editing or deleting an existing one.
type Migrator struct {
	DB       clickhouse.Conn
	FS       fs.FS
	Database string                       // defaults to "observability"
	Logger   func(format string, args ...any)
}

// MigrationStatus describes the apply state for a single migration file.
type MigrationStatus struct {
	Version string
	Applied bool
}

const migrationTrackingTable = "schema_migrations"

// Up applies every pending migration in lexical order, stopping on first error.
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
		for i, stmt := range splitMigrationStatements(string(body)) {
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

// Status returns the applied/pending state for every file, in apply order.
func (m *Migrator) Status(ctx context.Context) ([]MigrationStatus, error) {
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
	out := make([]MigrationStatus, len(files))
	for i, name := range files {
		_, ok := appliedSet[name]
		out[i] = MigrationStatus{Version: name, Applied: ok}
	}
	return out, nil
}

func (m *Migrator) ensureTrackingTable(ctx context.Context) error {
	if m.Database == "" {
		m.Database = "observability"
	}
	// The `observability` database is created by the first migration. On a
	// fresh cluster the tracking table needs the database to exist first;
	// CREATE IF NOT EXISTS makes both calls safe to re-run.
	if err := m.DB.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+m.Database); err != nil {
		return err
	}
	return m.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
		    version    String,
		    applied_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY version
	`, m.Database, migrationTrackingTable))
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
	rows, err := m.DB.Query(ctx, fmt.Sprintf("SELECT version FROM %s.%s", m.Database, migrationTrackingTable))
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
		fmt.Sprintf("INSERT INTO %s.%s (version) VALUES (?)", m.Database, migrationTrackingTable),
		version,
	)
}

func (m *Migrator) logf(format string, args ...any) {
	if m.Logger == nil {
		return
	}
	m.Logger(format, args...)
}

// splitMigrationStatements breaks a multi-statement SQL blob into individual
// statements. Strips `-- line comments`, ignores whitespace-only fragments,
// and splits on `;` outside single-quoted strings. Our DDL files don't embed
// semicolons inside string literals; if a future migration does, switch to
// a proper tokenizer.
func splitMigrationStatements(sql string) []string {
	var out []string
	var buf strings.Builder
	inSingle := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if !inSingle && c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if c == '\'' {
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
