package expr

import (
	"database/sql"
	"testing"
	"time"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

func f(v float64) *float64 { return &v }

func state(status string, lastNotified time.Time) models.MonitorStateRow {
	s := models.MonitorStateRow{Status: status}
	if !lastNotified.IsZero() {
		s.LastNotifiedAt = sql.NullTime{Time: lastNotified, Valid: true}
	}
	return s
}

func TestDecideTransitions(t *testing.T) {
	now := time.Now().UTC()
	cond := models.Conditions{Comparator: "above", AlertThreshold: f(100), WarnThreshold: f(50)}

	cases := []struct {
		name       string
		prev       string
		value      float64
		hasData    bool
		wantStatus string
		wantNotify bool
		wantRecov  bool
	}{
		{"ok stays ok", "ok", 10, true, "ok", false, false},
		{"ok to warn", "ok", 60, true, "warn", true, false},
		{"ok to alert", "ok", 150, true, "alert", true, false},
		{"alert stays alert", "alert", 150, true, "alert", false, false},
		{"alert recovers", "alert", 10, true, "ok", true, true},
		{"warn recovers", "warn", 10, true, "ok", true, true},
		{"warn escalates", "warn", 150, true, "alert", true, false},
		{"first eval defaults to no_data prev", "", 10, true, "ok", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := Decide(state(c.prev, time.Time{}), models.MonitorRow{}, cond, c.value, c.hasData, 0, now)
			if d.NewStatus != c.wantStatus || d.ShouldNotify != c.wantNotify || d.IsRecovery != c.wantRecov {
				t.Errorf("got {status:%s notify:%v recovery:%v}, want {%s %v %v}",
					d.NewStatus, d.ShouldNotify, d.IsRecovery, c.wantStatus, c.wantNotify, c.wantRecov)
			}
		})
	}
}

// Hysteresis: while alerting, value must drop to the recovery threshold
// before the monitor returns to ok.
func TestDecideHysteresis(t *testing.T) {
	now := time.Now().UTC()
	cond := models.Conditions{
		Comparator:        "above",
		AlertThreshold:    f(100),
		RecoveryThreshold: f(80),
	}

	d := Decide(state("alert", time.Time{}), models.MonitorRow{}, cond, 90, true, 0, now)
	if d.NewStatus != "alert" {
		t.Errorf("value above recovery threshold should hold alert, got %s", d.NewStatus)
	}
	d = Decide(state("alert", time.Time{}), models.MonitorRow{}, cond, 80, true, 0, now)
	if d.NewStatus != "ok" || !d.IsRecovery {
		t.Errorf("value at recovery threshold should recover, got %s recovery=%v", d.NewStatus, d.IsRecovery)
	}
}

func TestDecideNoData(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		noDataAs   string
		wantStatus string
	}{
		{"alert", "alert"},
		{"ok", "ok"},
		{"", "no_data"},
	}
	for _, c := range cases {
		cond := models.Conditions{Comparator: "above", AlertThreshold: f(100), NoDataAs: c.noDataAs}
		d := Decide(state("ok", time.Time{}), models.MonitorRow{}, cond, 0, false, 0, now)
		if d.NewStatus != c.wantStatus {
			t.Errorf("NoDataAs=%q: got %s, want %s", c.noDataAs, d.NewStatus, c.wantStatus)
		}
	}
}

func TestDecideRenotify(t *testing.T) {
	now := time.Now().UTC()
	cond := models.Conditions{Comparator: "above", AlertThreshold: f(100)}

	// Still alerting, last notified 10 min ago, renotify every 5 min.
	d := Decide(state("alert", now.Add(-10*time.Minute)), models.MonitorRow{}, cond, 150, true, 300, now)
	if !d.ShouldNotify || d.Transition {
		t.Errorf("expected renotify without transition, got notify=%v transition=%v", d.ShouldNotify, d.Transition)
	}
	// Last notified 1 min ago: too soon.
	d = Decide(state("alert", now.Add(-time.Minute)), models.MonitorRow{}, cond, 150, true, 300, now)
	if d.ShouldNotify {
		t.Error("renotify fired before interval elapsed")
	}
	// Renotify disabled.
	d = Decide(state("alert", now.Add(-10*time.Minute)), models.MonitorRow{}, cond, 150, true, 0, now)
	if d.ShouldNotify {
		t.Error("renotify fired with interval disabled")
	}
}

func TestDecideBelowComparator(t *testing.T) {
	now := time.Now().UTC()
	cond := models.Conditions{Comparator: "below", AlertThreshold: f(10)}

	d := Decide(state("ok", time.Time{}), models.MonitorRow{}, cond, 5, true, 0, now)
	if d.NewStatus != "alert" {
		t.Errorf("below comparator: got %s, want alert", d.NewStatus)
	}
	d = Decide(state("alert", time.Time{}), models.MonitorRow{}, cond, 20, true, 0, now)
	if d.NewStatus != "ok" {
		t.Errorf("below comparator recovery: got %s, want ok", d.NewStatus)
	}
}
