package explorer

import "fmt"

var validSignalTypes = map[string]bool{"span": true, "log": true, "metric": true}
var validAggregations = map[string]bool{"count": true, "avg": true, "p95": true, "p99": true, "sum": true}
var validModes = map[string]bool{"timeseries": true, "table": true}

// Service orchestrates exploration queries.
type Service struct {
	Repo Repository
}

// NewService creates a new explorer Service.
func NewService(repo Repository) *Service {
	return &Service{Repo: repo}
}

// Explore validates the request and delegates to the repository.
func (s *Service) Explore(teamID, startMs, endMs int64, req ExploreRequest) (*ExploreResult, error) {
	if !validSignalTypes[req.SignalType] {
		return nil, fmt.Errorf("invalid signalType: %s (must be span, log, or metric)", req.SignalType)
	}
	if !validAggregations[req.Aggregation] {
		return nil, fmt.Errorf("invalid aggregation: %s", req.Aggregation)
	}
	if !validModes[req.Mode] {
		return nil, fmt.Errorf("invalid mode: %s (must be timeseries or table)", req.Mode)
	}

	// Validate aggregation compatibility with signal type
	if req.SignalType == "log" && req.Aggregation != "count" {
		return nil, fmt.Errorf("logs only support count aggregation")
	}
	if req.SignalType == "span" && req.Aggregation == "sum" {
		return nil, fmt.Errorf("spans do not support sum aggregation")
	}

	var rows []map[string]any
	var err error

	if req.Mode == "timeseries" {
		rows, err = s.Repo.QueryTimeSeries(teamID, startMs, endMs, req)
	} else {
		rows, err = s.Repo.QueryTable(teamID, startMs, endMs, req)
	}
	if err != nil {
		return nil, err
	}

	columns := deriveColumns(req)

	return &ExploreResult{
		Mode:    req.Mode,
		Columns: columns,
		Rows:    rows,
	}, nil
}

func deriveColumns(req ExploreRequest) []string {
	var cols []string
	if req.Mode == "timeseries" {
		cols = append(cols, "ts")
	}
	if req.GroupBy != "" {
		cols = append(cols, "group_key")
	}
	cols = append(cols, "value")
	return cols
}
