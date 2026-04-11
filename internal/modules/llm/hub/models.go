package hub

import (
	"database/sql"
	"time"
)

type scoreRow struct {
	ID             int64
	TeamID         int64
	Name           string
	Value          float64
	TraceID        string
	SpanID         string
	SessionID      sql.NullString
	PromptTemplate sql.NullString
	Model          sql.NullString
	Source         string
	Rationale      sql.NullString
	CreatedAt      time.Time
}

type promptRow struct {
	ID          int64
	TeamID      int64
	Slug        string
	DisplayName string
	Body        string
	Version     int
	CreatedAt   time.Time
	UpdatedAt   sql.NullTime
}

type datasetRow struct {
	ID             int64
	TeamID         int64
	Name           string
	QuerySnapshot  sql.NullString
	StartTimeMs    int64
	EndTimeMs      int64
	RowCount       int
	PayloadJSON    string
	CreatedAt      time.Time
}
