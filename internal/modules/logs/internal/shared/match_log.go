package shared

import (
	"regexp"
	"strings"
	"time"
)

// MatchesLog applies the same filter dimensions as BuildLogWhere, in memory, for streamed rows.
func (f LogFilters) MatchesLog(l Log) bool {
	startMs, endMs := normalizeLogFilterTimeRange(f.StartMs, f.EndMs)
	startNs := uint64(startMs) * 1_000_000 //nolint:gosec // G115
	endNs := uint64(endMs) * 1_000_000     //nolint:gosec // G115
	if l.Timestamp < startNs || l.Timestamp > endNs {
		return false
	}

	if len(f.Severities) > 0 && !stringInSlice(l.SeverityText, f.Severities) {
		return false
	}
	if len(f.ExcludeSeverities) > 0 && stringInSlice(l.SeverityText, f.ExcludeSeverities) {
		return false
	}
	if len(f.Services) > 0 && !stringInSlice(l.ServiceName, f.Services) {
		return false
	}
	if len(f.ExcludeServices) > 0 && stringInSlice(l.ServiceName, f.ExcludeServices) {
		return false
	}
	if len(f.Hosts) > 0 && !stringInSlice(l.Host, f.Hosts) {
		return false
	}
	if len(f.ExcludeHosts) > 0 && stringInSlice(l.Host, f.ExcludeHosts) {
		return false
	}
	if len(f.Pods) > 0 && !stringInSlice(l.Pod, f.Pods) {
		return false
	}
	if len(f.Containers) > 0 && !stringInSlice(l.Container, f.Containers) {
		return false
	}
	if len(f.Environments) > 0 && !stringInSlice(l.Environment, f.Environments) {
		return false
	}
	if f.TraceID != "" && l.TraceID != f.TraceID {
		return false
	}
	if f.SpanID != "" && l.SpanID != f.SpanID {
		return false
	}
	if f.Search != "" {
		q := strings.TrimSpace(f.Search)
		body := l.Body
		if !strings.Contains(strings.ToLower(body), strings.ToLower(q)) {
			return false
		}
	}
	for _, af := range f.AttributeFilters {
		val, ok := l.AttributesString[af.Key]
		if !ok {
			return false
		}
		switch af.Op {
		case "neq":
			if val == af.Value {
				return false
			}
		case "contains":
			if !strings.Contains(strings.ToLower(val), strings.ToLower(af.Value)) {
				return false
			}
		case "regex":
			re, err := regexp.Compile(af.Value)
			if err != nil || !re.MatchString(val) {
				return false
			}
		default:
			if val != af.Value {
				return false
			}
		}
	}
	return true
}

func normalizeLogFilterTimeRange(startMs, endMs int64) (int64, int64) {
	const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	if startMs <= 0 || (endMs-startMs) > maxTimeRangeMs {
		startMs = endMs - maxTimeRangeMs
	}
	return startMs, endMs
}

func stringInSlice(s string, list []string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
