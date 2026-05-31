// Package query implements the three monitor query backends: metric (reads
// observability.metrics_1m), apm (reads observability.spans_1m), log (reads
// observability.logs). Each backend exposes Scalar (single-value evaluation)
// and Series (bucketed timeseries for the detail-page chart). The evaluator
// dispatches by monitor type via the Backends map.
package query

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// ScalarResult is the single value Scalar returns. HasData is false when the
// underlying CH query produced no rows (or below min_sample); the decide
// function reads this to apply the monitor's no_data disposition.
type ScalarResult struct {
	Value   float64
	HasData bool
}

// Point is one bucket of a Series response.
type Point struct {
	BucketMs int64   `json:"bucket_ms"`
	Value    float64 `json:"value"`
}

// Backend is the per-type query interface. Implementations: metric, apm, log.
type Backend interface {
	Scalar(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, scope models.Scope, cond models.Conditions, now time.Time) (ScalarResult, error)
	Series(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, scope models.Scope, cond models.Conditions, windowMs int64, now time.Time) ([]Point, error)
}

// Registry bundles the three backends so the evaluator and the monitors-series
// handler can dispatch without importing each backend directly.
type Registry struct {
	Metric Backend
	APM    Backend
	Log    Backend
}

// For returns the backend for the given monitor type. Returns nil + an error
// when the type isn't one of metric/apm/log.
func (r Registry) For(t string) (Backend, error) {
	switch t {
	case "metric":
		return r.Metric, nil
	case "apm":
		return r.APM, nil
	case "log":
		return r.Log, nil
	default:
		return nil, errors.New("unknown monitor type: " + t)
	}
}

// DecodeScope unwraps a monitors row's scope JSON.
func DecodeScope(row models.MonitorRow) models.Scope {
	var s models.Scope
	_ = json.Unmarshal(row.ScopeJSON, &s)
	return s
}

// DecodeQuery unwraps a monitors row's query JSON.
func DecodeQuery(row models.MonitorRow) models.MonitorQuery {
	var q models.MonitorQuery
	_ = json.Unmarshal(row.QueryJSON, &q)
	return q
}

// DecodeConditions unwraps a monitors row's conditions JSON.
func DecodeConditions(row models.MonitorRow) models.Conditions {
	var c models.Conditions
	_ = json.Unmarshal(row.ConditionsJSON, &c)
	return c
}

// scopeServiceTag returns the value of the first tag matching key (or "").
func scopeServiceTag(scope models.Scope, key string) string {
	for _, t := range scope.Tags {
		if t.Key == key {
			return t.Value
		}
	}
	return ""
}

// teamIDArg binds the team_id parameter as CH UInt32.
func teamIDArg(teamID int64) any {
	return clickhouse.Named("teamID", uint32(teamID)) //nolint:gosec // G115 — TeamID fits UInt32
}
