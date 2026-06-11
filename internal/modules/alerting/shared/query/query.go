// Package query implements the query backends (metric, apm, log)
// for monitor evaluation and timeseries generation.
package query

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// ScalarResult is the value returned by Scalar. HasData is false if
// the ClickHouse query returns no rows.
type ScalarResult struct {
	Value   float64
	HasData bool
}

// Point is one bucket of a Series response.
type Point struct {
	BucketMs int64   `json:"bucket_ms"`
	Value    float64 `json:"value"`
}

// Backend is the query interface implemented by metric, apm, and log.
type Backend interface {
	Scalar(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, scope models.Scope, cond models.Conditions, now time.Time) (ScalarResult, error)
	Series(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, scope models.Scope, cond models.Conditions, windowMs int64, now time.Time) ([]Point, error)
}

// Registry bundles the backends to allow evaluator and series dispatch.
type Registry struct {
	Metric Backend
	APM    Backend
	Log    Backend
}

// For returns the backend for the given monitor type.
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



// teamIDArg binds the team_id parameter as CH UInt32.
func teamIDArg(teamID int64) any {
	return clickhouse.Named("teamID", uint32(teamID))
}
