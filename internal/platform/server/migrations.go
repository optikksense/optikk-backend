package server

import (
	"database/sql"
	"fmt"
	"log"
)

// runWithAdvisoryLock acquires a MySQL advisory lock identified by lockName,
// runs fn, then releases the lock. This ensures that schema migrations run
// serially across all pods in a rolling Kubernetes deployment — preventing
// concurrent ALTER TABLE races that can cause duplicate-column errors.
//
// lockTimeout is the maximum number of seconds to wait for the lock before
// giving up. A timeout of 0 means wait indefinitely.
func runWithAdvisoryLock(db *sql.DB, lockName string, lockTimeout int, fn func()) {
	var acquired int
	err := db.QueryRow(`SELECT GET_LOCK(?, ?)`, lockName, lockTimeout).Scan(&acquired)
	if err != nil {
		log.Printf("WARN: advisory lock GET_LOCK(%q) error: %v — running migration without lock", lockName, err)
		fn()
		return
	}
	if acquired != 1 {
		log.Printf("WARN: advisory lock GET_LOCK(%q) timed out — skipping migration (another pod is migrating)", lockName)
		return
	}
	defer func() {
		if _, err := db.Exec(`SELECT RELEASE_LOCK(?)`, lockName); err != nil {
			log.Printf("WARN: advisory lock RELEASE_LOCK(%q) error: %v", lockName, err)
		}
	}()
	fn()
}

// runMigrations runs all schema migrations under a single MySQL advisory lock,
// ensuring only one pod performs schema changes at a time during rolling deploys.
func runMigrations(db *sql.DB, defaultRetentionDays int) {
	runWithAdvisoryLock(db, "optic_schema_migration", 30, func() {
		// Migrate users/teams schema: add teams JSON column, org_name, drop user_teams.
		migrateUserTeamsSchema(db)
		// Migrate teams table: add retention_days column for per-team retention policies.
		migrateTeamRetentionDays(db, defaultRetentionDays)
	})
}

// migrateTeamRetentionDays adds a retention_days column to the teams table.
// The column controls how many days of telemetry data to keep per team.
// Default is 30 days. Idempotent — safe to re-run.
func migrateTeamRetentionDays(db *sql.DB, defaultRetentionDays int) {
	if columnExists(db, "teams", "retention_days") {
		return
	}
	if defaultRetentionDays < 1 {
		defaultRetentionDays = 30
	}
	q := fmt.Sprintf(
		`ALTER TABLE teams ADD COLUMN retention_days INT NOT NULL DEFAULT %d AFTER api_key`,
		defaultRetentionDays,
	)
	if _, err := db.Exec(q); err != nil {
		log.Printf("WARN: migration add teams.retention_days: %v", err)
	} else {
		log.Printf("migration: added teams.retention_days column (default %d)", defaultRetentionDays)
	}
}

// migrateUserTeamsSchema migrates the users/teams tables to the new schema:
// - Adds teams JSON column to users (backfilled from user_teams)
// - Adds org_name column to teams
// - Drops organization_id from users
// - Drops user_teams table
// Each step is idempotent and safe to re-run.
func migrateUserTeamsSchema(db *sql.DB) {
	// Step 1: Add org_name to teams if missing.
	if !columnExists(db, "teams", "org_name") {
		if _, err := db.Exec(`ALTER TABLE teams ADD COLUMN org_name VARCHAR(100) DEFAULT NULL AFTER organization_id`); err != nil {
			log.Printf("WARN: migration add teams.org_name: %v", err)
		} else {
			log.Println("migration: added teams.org_name column")
			db.Exec(`UPDATE teams SET org_name = 'Default' WHERE org_name IS NULL`)
		}
	}

	// Step 2: Add teams JSON column to users if missing.
	if !columnExists(db, "users", "teams") {
		if _, err := db.Exec(`ALTER TABLE users ADD COLUMN teams JSON DEFAULT NULL AFTER role`); err != nil {
			log.Printf("WARN: migration add users.teams: %v", err)
		} else {
			log.Println("migration: added users.teams column")

			// Backfill from user_teams junction table if it exists.
			if tableExists(db, "user_teams") {
				if _, err := db.Exec(`
					UPDATE users u SET u.teams = (
						SELECT COALESCE(JSON_ARRAYAGG(JSON_OBJECT('team_id', ut.team_id, 'role', ut.role)), '[]')
						FROM user_teams ut WHERE ut.user_id = u.id
					)
				`); err != nil {
					log.Printf("WARN: migration backfill users.teams: %v", err)
				} else {
					log.Println("migration: backfilled users.teams from user_teams")
				}
			}

			// Set empty array for users with no teams.
			db.Exec(`UPDATE users SET teams = '[]' WHERE teams IS NULL`)
		}
	}

	// Step 3: Drop organization_id from users if it still exists.
	if columnExists(db, "users", "organization_id") {
		// Drop the index first (ignore error if it doesn't exist).
		db.Exec(`ALTER TABLE users DROP INDEX idx_user_org`)
		if _, err := db.Exec(`ALTER TABLE users DROP COLUMN organization_id`); err != nil {
			log.Printf("WARN: migration drop users.organization_id: %v", err)
		} else {
			log.Println("migration: dropped users.organization_id column")
		}
	}

	// Step 4: Drop user_teams table if it exists.
	if tableExists(db, "user_teams") {
		if _, err := db.Exec(`DROP TABLE user_teams`); err != nil {
			log.Printf("WARN: migration drop user_teams: %v", err)
		} else {
			log.Println("migration: dropped user_teams table")
		}
	}
}

func columnExists(db *sql.DB, table, column string) bool {
	var count int64
	err := db.QueryRow(`
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
	`, table, column).Scan(&count)
	return err == nil && count > 0
}

func tableExists(db *sql.DB, table string) bool {
	var count int64
	err := db.QueryRow(`
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
	`, table).Scan(&count)
	return err == nil && count > 0
}
