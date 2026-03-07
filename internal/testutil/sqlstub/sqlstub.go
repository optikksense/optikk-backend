package sqlstub

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
)

const driverName = "observability-sqlstub"

var registerDriver sync.Once

// OpenTeamDB returns a sql.DB that resolves any OTLP API key lookup to teamID
// and accepts arbitrary INSERT statements used by ingest queue flushes.
func OpenTeamDB(teamID int64) (*sql.DB, error) {
	registerDriver.Do(func() {
		sql.Register(driverName, stubDriver{})
	})

	return sql.Open(driverName, strconv.FormatInt(teamID, 10))
}

type stubDriver struct{}

func (stubDriver) Open(name string) (driver.Conn, error) {
	teamID, err := strconv.ParseInt(name, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse team id: %w", err)
	}
	return &stubConn{teamID: teamID}, nil
}

type stubConn struct {
	teamID int64
}

func (c *stubConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not supported")
}
func (c *stubConn) Close() error              { return nil }
func (c *stubConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("transactions not supported") }
func (c *stubConn) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

func (c *stubConn) Ping(context.Context) error { return nil }

func (c *stubConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "SELECT id FROM teams") {
		return &stubRows{
			columns: []string{"id"},
			values:  [][]driver.Value{{c.teamID}},
		}, nil
	}

	return &stubRows{}, nil
}

func (c *stubConn) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

type stubRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *stubRows) Columns() []string {
	return r.columns
}

func (r *stubRows) Close() error {
	return nil
}

func (r *stubRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}

	copy(dest, r.values[r.index])
	r.index++
	return nil
}
