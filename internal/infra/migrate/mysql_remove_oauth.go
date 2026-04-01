package migrate

import (
	"context"
	"database/sql"
	"fmt"
)

func mysqlUp002(ctx context.Context, db *sql.DB) error {
	for _, column := range []string{"oauth_provider", "oauth_id"} {
		exists, err := mysqlColumnExists(ctx, db, "observability", "users", column)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE observability.users DROP COLUMN %s", column)); err != nil {
			return fmt.Errorf("drop users.%s: %w", column, err)
		}
	}
	return nil
}

func mysqlDown002(context.Context, *sql.DB) error {
	return nil
}

func mysqlColumnExists(ctx context.Context, db *sql.DB, schemaName, tableName, columnName string) (bool, error) {
	var exists int
	if err := db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?`,
		schemaName,
		tableName,
		columnName,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("check column %s.%s.%s: %w", schemaName, tableName, columnName, err)
	}
	return exists > 0, nil
}
