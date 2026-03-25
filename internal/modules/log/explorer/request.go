package explorer

import (
	"strings"

	logshared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

func mapToLogFilters(req QueryRequest, teamID int64) logshared.LogFilters {
	params := req.Params
	filters := logshared.LogFilters{
		TeamID:            teamID,
		StartMs:           req.StartTime,
		EndMs:             req.EndTime,
		Severities:        params.Severities,
		Services:          params.Services,
		Hosts:             params.Hosts,
		Pods:              params.Pods,
		Containers:        params.Containers,
		Environments:      params.Environments,
		TraceID:           params.TraceID,
		SpanID:            params.SpanID,
		Search:            strings.TrimSpace(params.Search),
		SearchMode:        params.SearchMode,
		ExcludeSeverities: params.ExcludeSeverities,
		ExcludeServices:   params.ExcludeServices,
		ExcludeHosts:      params.ExcludeHosts,
	}
	filters.AttributeFilters = append(filters.AttributeFilters, params.AttributeFilters...)

	return filters
}
