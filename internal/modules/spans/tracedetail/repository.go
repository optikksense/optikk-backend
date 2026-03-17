package tracedetail

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	database "github.com/observability/observability-backend-go/internal/database"
)

var reNumberLiteral = regexp.MustCompile(`\b\d+(\.\d+)?\b`)
var reStringLiteral = regexp.MustCompile(`'[^']*'`)
var reMultiSpace = regexp.MustCompile(`\s+`)

func normalizeDBStatement(stmt string) string {
	if stmt == "" {
		return ""
	}
	s := reStringLiteral.ReplaceAllString(stmt, "?")
	s = reNumberLiteral.ReplaceAllString(s, "?")
	s = reMultiSpace.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

type Repository interface {
	GetSpanEvents(ctx context.Context, teamID int64, traceID string) ([]spanEventRow, []exceptionRow, error)
	GetSpanKindBreakdown(ctx context.Context, teamID int64, traceID string) ([]spanKindDurationRow, error)
	GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error)
	GetSpanSelfTimes(ctx context.Context, teamID int64, traceID string) ([]SpanSelfTime, error)
	GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error)
	GetSpanAttributes(ctx context.Context, teamID int64, traceID, spanID string) (*spanAttributeRow, error)
	GetRelatedTraces(ctx context.Context, teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	GetFlamegraphData(ctx context.Context, teamID int64, traceID string) ([]flamegraphRow, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}
