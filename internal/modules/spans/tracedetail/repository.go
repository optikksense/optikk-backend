package tracedetail

import (
	"regexp"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
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

type Repository interface {
	GetSpanEvents(teamID int64, traceID string) ([]SpanEvent, error)
	GetSpanKindBreakdown(teamID int64, traceID string) ([]SpanKindDuration, error)
	GetCriticalPath(teamID int64, traceID string) ([]CriticalPathSpan, error)
	GetSpanSelfTimes(teamID int64, traceID string) ([]SpanSelfTime, error)
	GetErrorPath(teamID int64, traceID string) ([]ErrorPathSpan, error)
	GetSpanAttributes(teamID int64, traceID, spanID string) (*SpanAttributes, error)
	GetRelatedTraces(teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error)
	GetFlamegraphData(teamID int64, traceID string) ([]FlamegraphFrame, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}
