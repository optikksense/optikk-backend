package alerts

import (
	"database/sql"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
)

// AlertFatigueMetrics is the response shape for GET /api/v1/alerts/fatigue.
type AlertFatigueMetrics struct {
	// Per-service alert noise breakdown (last windowDays days).
	ServiceNoise []ServiceNoiseRow `json:"service_noise"`
	// MTTD = Mean Time To Detect (created_at relative to anomaly start if available, else 0).
	MTTDMinutes float64 `json:"mttd_minutes"`
	// MTTR = Mean Time To Resolve.
	MTTRMinutes float64 `json:"mttr_minutes"`
	// Total firing alerts in window.
	TotalFiring int64 `json:"total_firing"`
	// Suppressed / auto-resolved within 5 min (noisy).
	NoisyCount int64 `json:"noisy_count"`
	// NoisyPct = NoisyCount / TotalFiring * 100.
	NoisyPct float64 `json:"noisy_pct"`
}

// ServiceNoiseRow is one row of the per-service breakdown.
type ServiceNoiseRow struct {
	Service     string  `json:"service"`
	Count       int64   `json:"count"`
	MTTRMinutes float64 `json:"mttr_minutes"`
	NoisyCount  int64   `json:"noisy_count"`
}

// GetAlertFatigueMetrics returns alert fatigue analytics for the team.
// Query params: window_days (default 7)
//
// GET /api/v1/alerts/fatigue?window_days=7
func (h *AlertHandler) GetAlertFatigueMetrics(c *gin.Context) {
	tenant := h.GetTenant(c)
	windowDays := ParseIntParam(c, "window_days", 7)
	if windowDays < 1 {
		windowDays = 1
	}
	if windowDays > 90 {
		windowDays = 90
	}
	since := time.Now().AddDate(0, 0, -windowDays)

	metrics, err := queryAlertFatigue(h.DB, tenant.TeamID, since)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to compute alert fatigue metrics")
		return
	}
	RespondOK(c, metrics)
}

// queryAlertFatigue runs the MTTD/MTTR + noise queries against MySQL.
func queryAlertFatigue(db *sql.DB, teamID int64, since time.Time) (*AlertFatigueMetrics, error) {
	metrics := &AlertFatigueMetrics{}

	// 1. MTTR and noisy-alert summary.
	type summary struct {
		total               int64
		noisy               int64 // resolved within 5 min of firing
		totalResolveMinutes float64
	}
	var s summary
	err := db.QueryRow(fmt.Sprintf(`
		SELECT
			COUNT(*) AS total,
			SUM(CASE WHEN status = 'resolved'
				AND TIMESTAMPDIFF(SECOND, created_at, updated_at) < 300
				THEN 1 ELSE 0 END) AS noisy,
			AVG(CASE WHEN status = 'resolved'
				THEN TIMESTAMPDIFF(MINUTE, created_at, updated_at)
				ELSE NULL END) AS avg_resolve_minutes
		FROM alerts
		WHERE team_id = ? AND created_at >= ?
	`), teamID, since).Scan(&s.total, &s.noisy, &s.totalResolveMinutes)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	metrics.TotalFiring = s.total
	metrics.NoisyCount = s.noisy
	if s.total > 0 {
		metrics.NoisyPct = float64(s.noisy) / float64(s.total) * 100
	}
	metrics.MTTRMinutes = s.totalResolveMinutes

	// 2. Per-service breakdown.
	rows, err := db.Query(`
		SELECT
			COALESCE(service_name, 'unknown') AS service,
			COUNT(*) AS cnt,
			AVG(CASE WHEN status = 'resolved'
				THEN TIMESTAMPDIFF(MINUTE, created_at, updated_at)
				ELSE NULL END) AS avg_mttr,
			SUM(CASE WHEN status = 'resolved'
				AND TIMESTAMPDIFF(SECOND, created_at, updated_at) < 300
				THEN 1 ELSE 0 END) AS noisy_cnt
		FROM alerts
		WHERE team_id = ? AND created_at >= ?
		GROUP BY service_name
		ORDER BY cnt DESC
		LIMIT 20
	`, teamID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var r ServiceNoiseRow
		if err := rows.Scan(&r.Service, &r.Count, &r.MTTRMinutes, &r.NoisyCount); err != nil {
			continue
		}
		metrics.ServiceNoise = append(metrics.ServiceNoise, r)
	}

	return metrics, nil
}
