package shared

import (
	"strings"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

func ParseFilters(c *gin.Context) LogFilters {
	f := LogFilters{
		Severities:        modulecommon.ParseListParam(c, "severities"),
		Services:          modulecommon.ParseListParam(c, "services"),
		Hosts:             modulecommon.ParseListParam(c, "hosts"),
		Pods:              modulecommon.ParseListParam(c, "pods"),
		Containers:        modulecommon.ParseListParam(c, "containers"),
		Environments:      modulecommon.ParseListParam(c, "environments"),
		TraceID:           c.Query("traceId"),
		SpanID:            c.Query("spanId"),
		Search:            c.Query("search"),
		SearchMode:        c.Query("searchMode"),
		ExcludeSeverities: modulecommon.ParseListParam(c, "excludeSeverities"),
		ExcludeServices:   modulecommon.ParseListParam(c, "excludeServices"),
		ExcludeHosts:      modulecommon.ParseListParam(c, "excludeHosts"),
	}

	for key, vals := range c.Request.URL.Query() {
		if len(vals) == 0 {
			continue
		}
		if after, ok := strings.CutPrefix(key, "attr."); ok {
			f.AttributeFilters = append(f.AttributeFilters, LogAttributeFilter{Key: after, Value: vals[0], Op: "eq"})
		}
		if after, ok := strings.CutPrefix(key, "attr_neq."); ok {
			f.AttributeFilters = append(f.AttributeFilters, LogAttributeFilter{Key: after, Value: vals[0], Op: "neq"})
		}
		if after, ok := strings.CutPrefix(key, "attr_contains."); ok {
			f.AttributeFilters = append(f.AttributeFilters, LogAttributeFilter{Key: after, Value: vals[0], Op: "contains"})
		}
		if after, ok := strings.CutPrefix(key, "attr_regex."); ok {
			f.AttributeFilters = append(f.AttributeFilters, LogAttributeFilter{Key: after, Value: vals[0], Op: "regex"})
		}
	}

	return f
}

func EnrichFilters(c *gin.Context, teamID int64) (LogFilters, bool) {
	f := ParseFilters(c)
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return LogFilters{}, false
	}
	f.TeamID = teamID
	f.StartMs = startMs
	f.EndMs = endMs
	return f, true
}
