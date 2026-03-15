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

const maxAlertWorkers = 10

func (e *Engine) evaluateAlerts() {
	mysqlDB := database.NewMySQLWrapper(e.db)
	rules, err := database.QueryMaps(mysqlDB,
		`SELECT id, team_id, name, severity, condition_type, signal_type, query, operator, threshold, duration_minutes, service_name
		 FROM alert_rules WHERE enabled = 1 LIMIT 5000`)
	if err != nil {
		log.Printf("alerting: error fetching rules: %v", err)
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxAlertWorkers)

	for _, rule := range rules {
		wg.Add(1)
		sem <- struct{}{}
		go func(rule map[string]any) {
			defer wg.Done()
			defer func() { <-sem }()
			e.evaluateRule(rule)
		}(rule)
	}

	wg.Wait()
}

func (e *Engine) evaluateRule(rule map[string]any) {
	ruleID := database.Int64FromAny(rule["id"])
	teamID := database.Int64FromAny(rule["team_id"])
	ruleName := database.StringFromAny(rule["name"])
	severity := database.StringFromAny(rule["severity"])
	signalType := database.StringFromAny(rule["signal_type"])
	operator := database.StringFromAny(rule["operator"])
	threshold := database.Float64FromAny(rule["threshold"])
	durationMin := database.Int64FromAny(rule["duration_minutes"])
	serviceName := database.StringFromAny(rule["service_name"])

	value, err := e.querySignal(teamID, signalType, serviceName, int(durationMin))
	if err != nil {
		log.Printf("alerting: rule %d query error: %v", ruleID, err)
		return
	}

	triggered := compareValue(value, operator, threshold)
	if !triggered {
		return
	}

	// Check if there's already an open incident for this rule
	mysqlDB := database.NewMySQLWrapper(e.db)
	existing, _ := database.QueryMap(mysqlDB,
		`SELECT id FROM alert_incidents WHERE rule_id = ? AND team_id = ? AND status = 'open' LIMIT 1`,
		ruleID, teamID)
	if existing != nil {
		return // already open
	}

	msg := fmt.Sprintf("Rule '%s' triggered: value=%.2f %s %.2f", ruleName, value, operator, threshold)
	_, err = e.db.Exec(
		`INSERT INTO alert_incidents (team_id, rule_id, rule_name, severity, status, trigger_value, threshold, message, triggered_at)
		 VALUES (?, ?, ?, ?, 'open', ?, ?, ?, NOW())`,
		teamID, ruleID, ruleName, severity, value, threshold, msg)
	if err != nil {
		log.Printf("alerting: rule %d incident insert error: %v", ruleID, err)
		return
	}

	e.notifyChannels(teamID, ruleName, severity, msg)
}

func (e *Engine) querySignal(teamID int64, signalType, serviceName string, durationMin int) (float64, error) {
	chDB := database.NewClickHouseWrapper(e.ch)
	var query string
	var args []any

	switch signalType {
	case "log":
		query = `SELECT count() AS val FROM observability.logs
			WHERE team_id = ? AND severity_text IN ('ERROR', 'FATAL')
			AND timestamp >= now() - INTERVAL ? MINUTE`
		args = []any{teamID, durationMin}
	case "span":
		query = `SELECT countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() AS val
			FROM observability.spans s
			WHERE s.team_id = ? AND s.timestamp >= now() - INTERVAL ? MINUTE`
		args = []any{teamID, durationMin}
		if serviceName != "" {
			query += ` AND s.service_name = ?`
			args = append(args, serviceName)
		}
	case "metric":
		query = `SELECT avg(value) AS val FROM observability.metrics
			WHERE team_id = ? AND timestamp >= now() - INTERVAL ? MINUTE`
		args = []any{teamID, durationMin}
	default:
		return 0, fmt.Errorf("unknown signal type: %s", signalType)
	}

	row, err := database.QueryMap(chDB, query, args...)
	if err != nil {
		return 0, err
	}
	return database.Float64FromAny(row["val"]), nil
}

func compareValue(value float64, operator string, threshold float64) bool {
	switch operator {
	case "gt":
		return value > threshold
	case "gte":
		return value >= threshold
	case "lt":
		return value < threshold
	case "lte":
		return value <= threshold
	case "eq":
		return value == threshold
	default:
		return false
	}
}

func (e *Engine) notifyChannels(teamID int64, ruleName, severity, message string) {
	mysqlDB := database.NewMySQLWrapper(e.db)
	channels, err := database.QueryMaps(mysqlDB,
		`SELECT channel_type, config FROM notification_channels WHERE team_id = ? AND enabled = 1`, teamID)
	if err != nil {
		log.Printf("alerting: error fetching channels for team %d: %v", teamID, err)
		return
	}

	for _, ch := range channels {
		chType := database.StringFromAny(ch["channel_type"])
		config := database.StringFromAny(ch["config"])

		switch chType {
		case "slack":
			var cfg struct{ URL string `json:"url"` }
			if json.Unmarshal([]byte(config), &cfg) == nil && cfg.URL != "" {
				text := fmt.Sprintf("*[%s] %s*\n%s", severity, ruleName, message)
				if err := e.sendSlackMessage(cfg.URL, text); err != nil {
					log.Printf("alerting: slack notify error: %v", err)
				}
			}
		case "webhook":
			var cfg struct{ URL string `json:"url"` }
			if json.Unmarshal([]byte(config), &cfg) == nil && cfg.URL != "" {
				e.sendWebhook(cfg.URL, ruleName, severity, message)
			}
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

func (e *Engine) sendWebhook(url, ruleName, severity, message string) {
	payload := map[string]string{
		"rule":     ruleName,
		"severity": severity,
		"message":  message,
	}
	body, _ := json.Marshal(payload)
	resp, err := e.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("alerting: webhook error: %v", err)
		return
	}
	resp.Body.Close()
}
