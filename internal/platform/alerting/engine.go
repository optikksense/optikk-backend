package alerting

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/observability/observability-backend-go/internal/database"
)

type Engine struct {
	db *sql.DB
	ch *sql.DB
}

func NewEngine(db, ch *sql.DB) *Engine {
	return &Engine{db: db, ch: ch}
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

func (e *Engine) evaluateAlerts() {
	teams, err := database.QueryMaps(database.NewMySQLWrapper(e.db), "SELECT id, name, slack_webhook_url FROM teams WHERE slack_webhook_url IS NOT NULL AND slack_webhook_url != ''")
	if err != nil {
		log.Printf("alerting: error fetching teams: %v", err)
		return
	}

	for _, team := range teams {
		teamID := team["id"].(int64)
		webhookURL := team["slack_webhook_url"].(string)
		teamName := team["name"].(string)

		// Static basic rule: > 10 errors in 5m
		query := `
            SELECT count() AS errors
            FROM observability.logs
            WHERE team_id = ? AND level IN ('ERROR', 'FATAL') AND timestamp >= now() - INTERVAL 5 MINUTE
        `
		res, err := database.QueryMap(database.NewMySQLWrapper(e.ch), query, teamID)
		if err != nil {
			log.Printf("alerting: error querying clickhouse for team %d: %v", teamID, err)
			continue
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
			if err := sendSlackMessage(webhookURL, msg); err != nil {
				log.Printf("alerting: failed to send slack message for team %d: %v", teamID, err)
			} else {
				log.Printf("alerting: successfully fired error rate alert to team %d", teamID)
			}
		}
	}
}

func sendSlackMessage(webhookURL, text string) error {
	payload := map[string]string{"text": text}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("slack returned status %d", resp.StatusCode)
	}
	return nil
}
