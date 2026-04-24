package database

const (
	// maxStmtBytes caps the db.statement attribute we stash on the
	// OTel span. SQL statements inside this codebase rarely exceed a
	// few KB; truncating keeps the span payload bounded regardless.
	maxStmtBytes = 4096
)

// resultLabel maps a DB error into the {ok,err} label used by the
// optikk_db_queries_total counter. Any non-nil error → "err".
func resultLabel(err error) string {
	if err != nil {
		return "err"
	}
	return "ok"
}

// truncateStmt returns stmt shortened to maxStmtBytes with an ellipsis
// marker if trimmed — kept for both CH and SQL instrument helpers.
func truncateStmt(stmt string) string {
	if len(stmt) <= maxStmtBytes {
		return stmt
	}
	return stmt[:maxStmtBytes] + "…[truncated]"
}
