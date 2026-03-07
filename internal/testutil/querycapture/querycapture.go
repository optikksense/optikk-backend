package querycapture

import (
	"context"
	"database/sql"
	"errors"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type CapturedCall struct {
	Query string
	Args  []any
}

// Querier records SQL calls and returns empty result sets by default.
type Querier struct {
	Queries   []CapturedCall
	QueryRows []CapturedCall
	Execs     []CapturedCall
	RowValues []any
}

func (q *Querier) Exec(query string, args ...any) (sql.Result, error) {
	q.Execs = append(q.Execs, CapturedCall{Query: query, Args: append([]any(nil), args...)})
	return rowsAffectedResult(0), nil
}

func (q *Querier) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, errors.New("exec context not implemented for querycapture.Querier")
}

func (q *Querier) Query(query string, args ...any) (dbutil.Rows, error) {
	q.Queries = append(q.Queries, CapturedCall{Query: query, Args: append([]any(nil), args...)})
	return Rows{}, nil
}

func (q *Querier) QueryRow(query string, args ...any) dbutil.Row {
	q.QueryRows = append(q.QueryRows, CapturedCall{Query: query, Args: append([]any(nil), args...)})
	values := q.RowValues
	if len(values) == 0 {
		values = []any{int64(0)}
	}
	return Row{Values: values}
}

func (q *Querier) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return nil, errors.New("transactions not implemented for querycapture.Querier")
}

func (q *Querier) Close() error {
	return nil
}

type Rows struct{}

func (Rows) Columns() []string { return []string{} }
func (Rows) Close() error      { return nil }
func (Rows) Next() bool        { return false }
func (Rows) Scan(...any)       {}

type Row struct {
	Values []any
}

func (r Row) Scan(dest ...any) error {
	for i := range dest {
		if i >= len(r.Values) {
			break
		}
		switch d := dest[i].(type) {
		case *int64:
			if v, ok := r.Values[i].(int64); ok {
				*d = v
			}
		}
	}
	return nil
}

type rowsAffectedResult int64

func (r rowsAffectedResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r rowsAffectedResult) RowsAffected() (int64, error) {
	return int64(r), nil
}
