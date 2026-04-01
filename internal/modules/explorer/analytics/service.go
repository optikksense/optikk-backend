package analytics

import (
	"context"
	"fmt"
	"math"
	"strconv"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
)

// analyticsRowDTO mirrors the existing trace analytics pattern: arrays of keys/values.
type analyticsRowDTO struct {
	DimensionKeys   []string  `ch:"dimension_keys"`
	DimensionValues []string  `ch:"dimension_values"`
	MetricKeys      []string  `ch:"metric_keys"`
	MetricValues    []float64 `ch:"metric_values"`
}

// Service handles unified analytics queries for both logs and traces.
type Service struct {
	db *dbutil.NativeQuerier
}

// NewService creates a new analytics service.
func NewService(db *dbutil.NativeQuerier) *Service {
	return &Service{db: db}
}

// RunQuery parses the query, builds SQL, executes it, and returns structured results.
func (s *Service) RunQuery(ctx context.Context, teamID int64, req AnalyticsRequest, scope string) (*AnalyticsResult, error) {
	cfg, schema, err := resolveScope(scope)
	if err != nil {
		return nil, err
	}

	// Override teamID in base where.
	origBase := cfg.BaseWhereFunc
	cfg.BaseWhereFunc = func(_ int64, startMs, endMs int64) (string, []any) {
		return origBase(teamID, startMs, endMs)
	}

	// Parse query string into WHERE clause.
	var queryWhere string
	var queryArgs []any
	if req.Query != "" {
		node, parseErr := queryparser.Parse(req.Query)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid query: %w", parseErr)
		}
		if node != nil {
			compiled, compileErr := queryparser.Compile(node, schema)
			if compileErr != nil {
				return nil, fmt.Errorf("query compilation error: %w", compileErr)
			}
			queryWhere = compiled.Where
			queryArgs = compiled.Args
		}
	}

	sql, args, err := BuildQuery(req, cfg, queryWhere, queryArgs)
	if err != nil {
		return nil, err
	}

	var rows []analyticsRowDTO
	if err := s.db.Select(ctx, &rows, sql, args...); err != nil {
		return nil, fmt.Errorf("analytics query failed: %w", err)
	}

	return buildResult(req, rows), nil
}

func resolveScope(scope string) (ScopeConfig, queryparser.SchemaResolver, error) {
	switch scope {
	case "logs":
		return LogsScopeConfig(), queryparser.LogsSchema{}, nil
	case "traces":
		return TracesScopeConfig(), queryparser.TracesSchema{}, nil
	default:
		return ScopeConfig{}, nil, fmt.Errorf("unknown scope: %q", scope)
	}
}

func buildResult(req AnalyticsRequest, rows []analyticsRowDTO) *AnalyticsResult {
	isTimeseries := req.VizMode == VizTimeseries && req.Step != ""

	var columns []string
	if isTimeseries {
		columns = append(columns, "time_bucket")
	}
	columns = append(columns, req.GroupBy...)
	for _, a := range req.Aggregations {
		columns = append(columns, a.Alias)
	}

	resultRows := make([]AnalyticsRow, 0, len(rows))
	for _, row := range rows {
		cells := make([]AnalyticsCell, 0, len(row.DimensionKeys)+len(row.MetricKeys))
		for i, key := range row.DimensionKeys {
			if i < len(row.DimensionValues) {
				cells = append(cells, cellFromString(key, row.DimensionValues[i]))
			}
		}
		for i, key := range row.MetricKeys {
			if i < len(row.MetricValues) {
				v := row.MetricValues[i]
				if math.IsNaN(v) || math.IsInf(v, 0) {
					v = 0
				}
				rounded := math.Round(v*100) / 100
				cells = append(cells, AnalyticsCell{Key: key, Type: ValueNumber, NumberValue: &rounded})
			}
		}
		resultRows = append(resultRows, AnalyticsRow{Cells: cells})
	}

	return &AnalyticsResult{
		Columns: columns,
		Rows:    resultRows,
	}
}

func cellFromString(key, val string) AnalyticsCell {
	// Try to parse as integer first, then float.
	if i, err := strconv.ParseInt(val, 10, 64); err == nil {
		return AnalyticsCell{Key: key, Type: ValueInteger, IntegerValue: &i}
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		rounded := math.Round(f*100) / 100
		return AnalyticsCell{Key: key, Type: ValueNumber, NumberValue: &rounded}
	}
	s := val
	return AnalyticsCell{Key: key, Type: ValueString, StringValue: &s}
}
