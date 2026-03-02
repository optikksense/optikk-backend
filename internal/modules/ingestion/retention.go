package telemetry

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"
)

// retentionTable describes a ClickHouse table whose rows should be purged
// based on the team's retention policy. The tsColumn is the timestamp column
// used in the WHERE clause for deletion.
type retentionTable struct {
	Name     string
	TSColumn string
}

// defaultRetentionTables lists the ClickHouse tables managed by the
// RetentionManager and the timestamp column used for age-based deletion.
var defaultRetentionTables = []retentionTable{
	{Name: "spans", TSColumn: "start_time"},
	{Name: "logs", TSColumn: "timestamp"},
	{Name: "metrics", TSColumn: "timestamp"},
	{Name: "deployments", TSColumn: "deploy_time"},
	{Name: "health_check_results", TSColumn: "timestamp"},
	{Name: "ai_requests", TSColumn: "timestamp"},
}

// RetentionManager periodically checks each team's retention policy in MySQL
// and deletes expired data from ClickHouse. It enforces a minimum retention
// of 1 day to prevent accidental data loss.
type RetentionManager struct {
	mysql              *sql.DB
	clickhouse         *sql.DB
	defaultRetention   int // days
	interval           time.Duration
	minRetentionDays   int

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// RetentionManagerConfig controls the RetentionManager's behavior.
type RetentionManagerConfig struct {
	// DefaultRetentionDays is used when a team has no explicit retention set.
	DefaultRetentionDays int
	// Interval controls how often the manager runs its sweep. Default: 1 hour.
	Interval time.Duration
	// MinRetentionDays is the absolute minimum retention period. Default: 1.
	MinRetentionDays int
}

// NewRetentionManager creates a new RetentionManager. Call Start() to begin
// the periodic sweep loop.
func NewRetentionManager(mysql, clickhouse *sql.DB, cfg RetentionManagerConfig) *RetentionManager {
	if cfg.DefaultRetentionDays < 1 {
		cfg.DefaultRetentionDays = 30
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 1 * time.Hour
	}
	if cfg.MinRetentionDays < 1 {
		cfg.MinRetentionDays = 1
	}
	return &RetentionManager{
		mysql:            mysql,
		clickhouse:       clickhouse,
		defaultRetention: cfg.DefaultRetentionDays,
		interval:         cfg.Interval,
		minRetentionDays: cfg.MinRetentionDays,
	}
}

// teamRetention holds a team's ID (as the ClickHouse UUID-formatted string)
// and its retention period in days.
type teamRetention struct {
	TeamID        string
	RetentionDays int
}

// Start launches the background goroutine that periodically enforces retention.
// It runs immediately on startup, then every configured interval.
func (rm *RetentionManager) Start(ctx context.Context) {
	ctx, rm.cancel = context.WithCancel(ctx)
	rm.wg.Add(1)
	go func() {
		defer rm.wg.Done()
		log.Printf("retention: manager started (interval=%s, default=%dd, min=%dd)",
			rm.interval, rm.defaultRetention, rm.minRetentionDays)

		// Run immediately on startup.
		rm.sweep(ctx)

		ticker := time.NewTicker(rm.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("retention: manager stopped")
				return
			case <-ticker.C:
				rm.sweep(ctx)
			}
		}
	}()
}

// Stop cancels the background goroutine and waits for it to finish.
func (rm *RetentionManager) Stop() {
	if rm.cancel != nil {
		rm.cancel()
	}
	rm.wg.Wait()
}

// sweep loads all teams from MySQL and deletes expired data for each.
func (rm *RetentionManager) sweep(ctx context.Context) {
	teams, err := rm.loadTeamRetentions(ctx)
	if err != nil {
		log.Printf("retention: failed to load team policies: %v", err)
		return
	}
	if len(teams) == 0 {
		return
	}
	log.Printf("retention: sweeping %d teams", len(teams))

	for _, t := range teams {
		if ctx.Err() != nil {
			return
		}
		rm.enforceForTeam(ctx, t)
	}
}

// loadTeamRetentions reads all active teams and their retention_days from MySQL.
// Teams are mapped to their ClickHouse UUID format (zero-padded).
func (rm *RetentionManager) loadTeamRetentions(ctx context.Context) ([]teamRetention, error) {
	// The retention_days column may not exist yet if the migration hasn't run.
	// Use IFNULL + a column-existence check isn't needed: the migration runs
	// before the RetentionManager starts. If somehow it's missing, the query
	// will fail and we return the error.
	rows, err := rm.mysql.QueryContext(ctx,
		`SELECT id, retention_days FROM teams WHERE active = 1`,
	)
	if err != nil {
		return nil, fmt.Errorf("query teams: %w", err)
	}
	defer rows.Close()

	var teams []teamRetention
	for rows.Next() {
		var id int64
		var days int
		if err := rows.Scan(&id, &days); err != nil {
			log.Printf("retention: scan team row: %v", err)
			continue
		}
		if days < rm.minRetentionDays {
			days = rm.minRetentionDays
		}
		// Convert numeric team ID to the ClickHouse UUID format used in telemetry tables.
		teamUUID := fmt.Sprintf("00000000-0000-0000-0000-%012d", id)
		teams = append(teams, teamRetention{TeamID: teamUUID, RetentionDays: days})
	}
	return teams, rows.Err()
}

// enforceForTeam deletes data older than the team's retention period from all
// managed ClickHouse tables.
func (rm *RetentionManager) enforceForTeam(ctx context.Context, t teamRetention) {
	for _, tbl := range defaultRetentionTables {
		if ctx.Err() != nil {
			return
		}

		// ALTER TABLE ... DELETE is a lightweight mutation in ClickHouse.
		// It marks parts for deletion asynchronously. This is the recommended
		// approach for conditional deletes; it does not block reads.
		query := fmt.Sprintf(
			"ALTER TABLE %s DELETE WHERE team_id = ? AND %s < now() - INTERVAL ? DAY",
			tbl.Name, tbl.TSColumn,
		)

		if _, err := rm.clickhouse.ExecContext(ctx, query, t.TeamID, t.RetentionDays); err != nil {
			log.Printf("retention: delete %s team=%s retention=%dd: %v",
				tbl.Name, t.TeamID, t.RetentionDays, err)
		}
	}
}
