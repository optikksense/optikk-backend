package shared

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
)

func ParseFilters(c *gin.Context) LogFilters {
	f := LogFilters{
		Severities:        common.ParseListParam(c, "severities"),
		Services:          common.ParseListParam(c, "services"),
		Hosts:             common.ParseListParam(c, "hosts"),
		Pods:              common.ParseListParam(c, "pods"),
		Containers:        common.ParseListParam(c, "containers"),
		Environments:      common.ParseListParam(c, "environments"),
		TraceID:           c.Query("traceId"),
		SpanID:            c.Query("spanId"),
		Search:            c.Query("search"),
		SearchMode:        c.Query("searchMode"),
		ExcludeSeverities: common.ParseListParam(c, "excludeSeverities"),
		ExcludeServices:   common.ParseListParam(c, "excludeServices"),
		ExcludeHosts:      common.ParseListParam(c, "excludeHosts"),
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
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return LogFilters{}, false
	}
	f.TeamID = teamID
	f.StartMs = startMs
	f.EndMs = endMs
	return f, true
}
