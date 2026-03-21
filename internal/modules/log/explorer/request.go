package explorer

import (
	"fmt"
	"strings"

	logshared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

func mapToLogFilters(req QueryRequest, teamID int64) logshared.LogFilters {
	params := req.Params
	filters := logshared.LogFilters{
		TeamID:            teamID,
		StartMs:           req.StartTime,
		EndMs:             req.EndTime,
		Severities:        mapToStringSlice(params["severities"]),
		Services:          mapToStringSlice(params["services"]),
		Hosts:             mapToStringSlice(params["hosts"]),
		Pods:              mapToStringSlice(params["pods"]),
		Containers:        mapToStringSlice(params["containers"]),
		Environments:      mapToStringSlice(params["environments"]),
		TraceID:           mapToString(params["traceId"]),
		SpanID:            mapToString(params["spanId"]),
		Search:            strings.TrimSpace(mapToString(params["search"])),
		SearchMode:        mapToString(params["searchMode"]),
		ExcludeSeverities: mapToStringSlice(params["excludeSeverities"]),
		ExcludeServices:   mapToStringSlice(params["excludeServices"]),
		ExcludeHosts:      mapToStringSlice(params["excludeHosts"]),
	}

	for key, value := range params {
		if strings.HasPrefix(key, "attr.") {
			filters.AttributeFilters = append(filters.AttributeFilters, logshared.LogAttributeFilter{
				Key:   strings.TrimPrefix(key, "attr."),
				Value: mapToString(value),
				Op:    "eq",
			})
		}
		if strings.HasPrefix(key, "attr_neq.") {
			filters.AttributeFilters = append(filters.AttributeFilters, logshared.LogAttributeFilter{
				Key:   strings.TrimPrefix(key, "attr_neq."),
				Value: mapToString(value),
				Op:    "neq",
			})
		}
		if strings.HasPrefix(key, "attr_contains.") {
			filters.AttributeFilters = append(filters.AttributeFilters, logshared.LogAttributeFilter{
				Key:   strings.TrimPrefix(key, "attr_contains."),
				Value: mapToString(value),
				Op:    "contains",
			})
		}
		if strings.HasPrefix(key, "attr_regex.") {
			filters.AttributeFilters = append(filters.AttributeFilters, logshared.LogAttributeFilter{
				Key:   strings.TrimPrefix(key, "attr_regex."),
				Value: mapToString(value),
				Op:    "regex",
			})
		}
	}

	return filters
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
