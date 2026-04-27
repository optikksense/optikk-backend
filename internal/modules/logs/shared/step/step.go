// Package step holds time-bucket helpers shared between log_analytics and
// log_trends. Adaptive defaults: ≤3h → 1m, ≤24h → 5m, ≤7d → 1h, else 1d.
package step

import (
	"strings"
	"time"
)

// ResolveStepMinutes picks a CH-friendly step size in minutes, honoring an
// explicit token when provided and adaptive defaults otherwise. Never returns
// less than tierStepMin so rollup tier alignment is preserved.
func ResolveStepMinutes(step string, tierStepMin int64, startMs, endMs int64) int64 {
	s := strings.TrimSpace(step)
	if s == "" {
		return Adaptive(tierStepMin, startMs, endMs)
	}
	explicit, ok := map[string]int64{"1m": 1, "5m": 5, "15m": 15, "1h": 60, "1d": 1440}[s]
	if !ok {
		return Adaptive(tierStepMin, startMs, endMs)
	}
	if explicit < tierStepMin {
		return tierStepMin
	}
	return explicit
}

// Adaptive picks a step purely from the time range.
func Adaptive(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var desired int64
	switch {
	case hours <= 3:
		desired = 1
	case hours <= 24:
		desired = 5
	case hours <= 168:
		desired = 60
	default:
		desired = 1440
	}
	if tierStepMin > desired {
		return tierStepMin
	}
	return desired
}

// FormatBucket truncates ts to the chosen stepMin and formats `YYYY-MM-DD HH:MM:SS`.
func FormatBucket(ts time.Time, stepMin int64) string {
	utc := ts.UTC()
	switch {
	case stepMin >= 1440:
		utc = time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
	default:
		secs := stepMin * 60
		utc = utc.Truncate(time.Duration(secs) * time.Second)
	}
	return utc.Format("2006-01-02 15:04:05")
}
