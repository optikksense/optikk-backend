package alerting

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/observability/observability-backend-go/internal/database"
)

type Engine struct {
	db     *sql.DB
	ch     *sql.DB
	client *http.Client
}

func NewEngine(db, ch *sql.DB) *Engine {
	return &Engine{
		db: db,
		ch: ch,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (e *Engine) Start() {
	go e.loop()
}

func (e *Engine) loop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		e.evaluateAlerts()
	}
}

// maxAlertWorkers limits concurrency for parallel team evaluation.
const maxAlertWorkers = 10

func (e *Engine) evaluateAlerts() {
	teams, err := database.QueryMaps(database.NewMySQLWrapper(e.db), "SELECT id, name, slack_webhook_url FROM teams WHERE slack_webhook_url IS NOT NULL AND slack_webhook_url != '' LIMIT 1000")
	if err != nil {
		log.Printf("alerting: error fetching teams: %v", err)
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxAlertWorkers)

	for _, team := range teams {
		teamID := database.Int64FromAny(team["id"])
		webhookURL := database.StringFromAny(team["slack_webhook_url"])
		teamName := database.StringFromAny(team["name"])

		if teamID == 0 || webhookURL == "" {
			continue
		}

		wg.Add(1)
		sem <- struct{}{} // acquire worker slot
		go func(teamID int64, teamName, webhookURL string) {
			defer wg.Done()
			defer func() { <-sem }() // release worker slot

			e.evaluateTeam(teamID, teamName, webhookURL)
		}(teamID, teamName, webhookURL)
	}

	wg.Wait()
}

func (e *Engine) evaluateTeam(teamID int64, teamName, webhookURL string) {
	query := `
		SELECT count() AS errors
		FROM observability.logs
		WHERE team_id = ? AND level IN ('ERROR', 'FATAL') AND timestamp >= now() - INTERVAL 5 MINUTE
	`
	res, err := database.QueryMap(database.NewClickHouseWrapper(e.ch), query, teamID)
	if err != nil {
		log.Printf("alerting: error querying clickhouse for team %d: %v", teamID, err)
		return
	}

	var errorCount uint64
	if v, ok := res["errors"]; ok {
		switch vals := v.(type) {
		case uint64:
			errorCount = vals
		case int64:
			errorCount = uint64(vals)
		case float64:
			errorCount = uint64(vals)
		case string:
			errorCount = uint64(database.MustAtoi64(vals, 0))
		}
	}

	if errorCount > 10 {
		msg := fmt.Sprintf("🚨 *Optikk Alert: High Error Rate* 🚨\nTeam: *%s*\nDetected *%d errors* in the last 5 minutes. Please investigate immediate log queries.", teamName, errorCount)
		start := time.Now()
		if err := e.sendSlackMessage(webhookURL, msg); err != nil {
			log.Printf("alerting: failed to send slack message for team %d (took %v): %v", teamID, time.Since(start), err)
		} else {
			log.Printf("alerting: fired error rate alert to team %d (took %v)", teamID, time.Since(start))
		}
	}
}

func (e *Engine) sendSlackMessage(webhookURL, text string) error {
	payload := map[string]string{"text": text}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := e.client.Post(webhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}
	return nil
}
