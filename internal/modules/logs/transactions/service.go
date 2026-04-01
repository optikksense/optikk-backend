package transactions

import (
	"context"
	"fmt"
	"strings"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	logshared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

// Transaction represents a group of related logs.
type Transaction struct {
	GroupValue  string   `json:"group_value"  ch:"group_value"`
	LogCount    int64    `json:"log_count"    ch:"log_count"`
	DurationNs  int64   `json:"duration_ns"  ch:"duration_ns"`
	MaxSeverity uint8    `json:"max_severity" ch:"max_severity"`
	FirstSeen   uint64   `json:"first_seen"   ch:"first_seen"`
	LastSeen    uint64   `json:"last_seen"    ch:"last_seen"`
	Services    []string `json:"services"     ch:"services"`
}

type TransactionsResponse struct {
	GroupByField string        `json:"group_by_field"`
	Transactions []Transaction `json:"transactions"`
}

type Service struct {
	db *dbutil.NativeQuerier
}

func NewService(db *dbutil.NativeQuerier) *Service {
	return &Service{db: db}
}

func (s *Service) GetTransactions(ctx context.Context, f logshared.LogFilters, groupByField string, limit int) (TransactionsResponse, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	where, args := logshared.BuildLogWhere(f)

	// Resolve the group-by field to a ClickHouse column expression.
	colExpr, err := resolveGroupField(groupByField)
	if err != nil {
		return TransactionsResponse{}, err
	}

	query := fmt.Sprintf(`
		SELECT
			%s AS group_value,
			count() AS log_count,
			toInt64(max(timestamp) - min(timestamp)) AS duration_ns,
			max(severity_number) AS max_severity,
			min(timestamp) AS first_seen,
			max(timestamp) AS last_seen,
			groupUniqArray(service) AS services
		FROM observability.logs
		WHERE %s AND %s != ''
		GROUP BY group_value
		ORDER BY log_count DESC
		LIMIT %d`, colExpr, where, colExpr, limit)

	var rows []Transaction
	if err := s.db.Select(ctx, &rows, query, args...); err != nil {
		return TransactionsResponse{}, fmt.Errorf("transactions query failed: %w", err)
	}

	return TransactionsResponse{
		GroupByField: groupByField,
		Transactions: rows,
	}, nil
}

// resolveGroupField maps a user-facing field name to a ClickHouse column expression.
func resolveGroupField(field string) (string, error) {
	switch strings.ToLower(field) {
	case "trace_id":
		return "trace_id", nil
	case "span_id":
		return "span_id", nil
	case "service":
		return "service", nil
	case "host":
		return "host", nil
	case "pod":
		return "pod", nil
	case "environment":
		return "environment", nil
	default:
		// Custom attribute field: @key
		if strings.HasPrefix(field, "@") {
			attrKey := field[1:]
			return "attributes_string['" + strings.ReplaceAll(attrKey, "'", "\\'") + "']", nil
		}
		return "", fmt.Errorf("invalid group_by_field: %q (use trace_id, service, host, pod, environment, or @attribute_name)", field)
	}
}
