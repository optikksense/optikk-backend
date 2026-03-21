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
		Services:    mapToStringSlice(params["services"]),
		Status:      mapToString(params["status"]),
		SearchText:  strings.TrimSpace(mapToString(params["search"])),
		MinDuration: mapToString(params["minDuration"]),
		MaxDuration: mapToString(params["maxDuration"]),
		TraceID:     mapToString(params["traceId"]),
		Operation:   mapToString(params["operationName"]),
		HTTPMethod:  mapToString(params["httpMethod"]),
		HTTPStatus:  mapToString(params["httpStatusCode"]),
		SearchMode:  defaultString(mapToString(params["mode"]), "all"),
		SpanKind:    mapToString(params["spanKind"]),
		SpanName:    mapToString(params["spanName"]),
	}
}

func mapToLiveTailFilters(params map[string]any) spanlivetail.LiveTailFilters {
	return spanlivetail.LiveTailFilters{
		Services:   mapToStringSlice(params["services"]),
		Status:     mapToString(params["status"]),
		SpanKind:   mapToString(params["spanKind"]),
		SearchText: strings.TrimSpace(mapToString(params["search"])),
		Operation:  mapToString(params["operationName"]),
		HTTPMethod: mapToString(params["httpMethod"]),
	}
}

func mapToString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case float64:
		return fmt.Sprintf("%.0f", typed)
	case nil:
		return ""
	default:
		return fmt.Sprint(value)
	}
}

func mapToStringSlice(value any) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		items := make([]string, 0, len(typed))
		for _, entry := range typed {
			next := strings.TrimSpace(mapToString(entry))
			if next != "" {
				items = append(items, next)
			}
		}
		return items
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return []string{strings.TrimSpace(typed)}
	default:
		return nil
	}
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
