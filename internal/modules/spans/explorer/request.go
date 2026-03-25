package explorer

import (
	"fmt"
	"strings"

	spanlivetail "github.com/observability/observability-backend-go/internal/modules/spans/livetail"
	spantraces "github.com/observability/observability-backend-go/internal/modules/spans/traces"
)

func mapToTraceFilters(req QueryRequest, teamID int64) spantraces.TraceFilters {
	params := req.Params
	return spantraces.TraceFilters{
		TeamID:      teamID,
		StartMs:     req.StartTime,
		EndMs:       req.EndTime,
		Services:    params.Services,
		Status:      params.Status,
		SearchText:  strings.TrimSpace(params.Search),
		MinDuration: formatOptionalNumber(params.MinDurationMs),
		MaxDuration: formatOptionalNumber(params.MaxDurationMs),
		TraceID:     params.TraceID,
		Operation:   params.OperationName,
		HTTPMethod:  params.HTTPMethod,
		HTTPStatus:  params.HTTPStatusCode,
		SearchMode:  defaultString(params.Mode, "all"),
		SpanKind:    params.SpanKind,
		SpanName:    params.SpanName,
	}
}

func mapToLiveTailFilters(params TraceExplorerParams) spanlivetail.LiveTailFilters {
	return spanlivetail.LiveTailFilters{
		Services:   params.Services,
		Status:     params.Status,
		SpanKind:   params.SpanKind,
		SearchText: strings.TrimSpace(params.Search),
		Operation:  params.OperationName,
		HTTPMethod: params.HTTPMethod,
	}
}

func formatOptionalNumber(value *float64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%.0f", *value)
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
