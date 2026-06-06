// Package filter defines the Filters shape, canonical resource-key
// resolution, and SQL clause emission for the metrics read path.
package filter

import (
	"errors"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000

// Filters is the typed shape consumed by the metrics repository.
type Filters struct {
	TeamID  int64
	StartMs int64
	EndMs   int64

	MetricName  string
	Aggregation string
	Step        string
	GroupBy     []string

	Tags []TagFilter
}

// TagFilter represents a single filter row with SQL-form operators.
type TagFilter struct {
	Key      string
	Operator string
	Values   []string
}

// validAggregations is the set of supported aggregation functions.
var validAggregations = map[string]bool{
	"avg": true, "sum": true, "min": true, "max": true, "count": true,
	"p50": true, "p75": true, "p95": true, "p99": true,
	"rate": true,
}

// IsValidAggregation reports whether agg is one of the supported
// aggregation function names.
func IsValidAggregation(agg string) bool {
	return validAggregations[agg]
}

// Validate clamps to ≤30 days, defaults Aggregation, rejects empty
// MetricName, missing time range, and unknown aggregation.
func (f *Filters) Validate() error {
	if f.MetricName == "" {
		return errors.New("metricName is required")
	}
	if f.StartMs <= 0 || f.EndMs <= 0 {
		return errors.New("startTime and endTime are required")
	}
	if f.EndMs < f.StartMs {
		return errors.New("endTime must be >= startTime")
	}
	if f.EndMs-f.StartMs > maxTimeRangeMs {
		f.StartMs = f.EndMs - maxTimeRangeMs
	}
	if f.Aggregation == "" {
		f.Aggregation = "avg"
	}
	if !validAggregations[f.Aggregation] {
		return errors.New("unsupported aggregation: " + f.Aggregation)
	}
	return nil
}

// keyAliases maps frontend resource aliases to their canonical key.
var keyAliases = map[string]string{
	"service":                "service",
	"service.name":           "service",
	"host":                   "host",
	"host.name":              "host",
	"environment":            "environment",
	"deployment.environment": "environment",
	"k8s_namespace":          "k8s_namespace",
	"k8s.namespace.name":     "k8s_namespace",
}

// Canonical maps a frontend resource alias to its canonical key, or "" if
// the key is not a known resource dimension.
func Canonical(key string) string {
	return keyAliases[key]
}

// ResourceColumn returns the low-cardinality column for a resource key.
func ResourceColumn(canonical string) string {
	switch canonical {
	case "service":
		return "service"
	case "host":
		return "host"
	case "environment":
		return "environment"
	case "k8s_namespace":
		return "k8s_namespace"
	}
	return ""
}

// AttrColumn returns the attributes JSON column expression for a tag key.
func AttrColumn(key string) string {
	return "attributes.`" + SanitizeKey(key) + "`::String"
}

// SanitizeKey strips characters outside [a-zA-Z0-9._-] to prevent injection.
func SanitizeKey(key string) string {
	var b strings.Builder
	b.Grow(len(key))
	for _, r := range key {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '.' || r == '_' || r == '-' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// validOperators is the set of SQL-form operators BuildClauses accepts.
var validOperators = map[string]bool{
	"=":      true,
	"!=":     true,
	"IN":     true,
	"NOT IN": true,
}

// BuildClauses partitions f.Tags into resource and attribute clauses.
func BuildClauses(f Filters) (resourceWhere, attrWhere string, args []any) {
	type resourceAccum struct {
		positive []string
		negative []string
	}
	resAccum := map[string]*resourceAccum{
		"service":       {},
		"host":          {},
		"environment":   {},
		"k8s_namespace": {},
	}

	rowIdx := 0
	for _, t := range f.Tags {
		if !validOperators[t.Operator] || len(t.Values) == 0 {
			continue
		}
		if canonical := Canonical(t.Key); canonical != "" {
			acc := resAccum[canonical]
			if acc == nil {
				continue
			}
			negated := t.Operator == "!=" || t.Operator == "NOT IN"
			if negated {
				acc.negative = append(acc.negative, t.Values...)
			} else {
				acc.positive = append(acc.positive, t.Values...)
			}
			continue
		}

		col := AttrColumn(t.Key)
		bind := "mf" + strconv.Itoa(rowIdx)
		rowIdx++
		switch t.Operator {
		case "=":
			attrWhere += " AND " + col + " = @" + bind
			args = append(args, clickhouse.Named(bind, t.Values[0]))
		case "!=":
			attrWhere += " AND " + col + " != @" + bind
			args = append(args, clickhouse.Named(bind, t.Values[0]))
		case "IN":
			attrWhere += " AND " + col + " IN @" + bind
			args = append(args, clickhouse.Named(bind, t.Values))
		case "NOT IN":
			attrWhere += " AND " + col + " NOT IN @" + bind
			args = append(args, clickhouse.Named(bind, t.Values))
		}
	}

	// Emit resource clauses in a fixed order for plan cache hits.
	for _, spec := range []struct {
		canonical string
		col       string
		posBind   string
		negBind   string
	}{
		{"service", "service", "services", "excServices"},
		{"host", "host", "hosts", "excHosts"},
		{"environment", "environment", "environments", "excEnvironments"},
		{"k8s_namespace", "k8s_namespace", "k8sNamespaces", "excK8sNamespaces"},
	} {
		acc := resAccum[spec.canonical]
		if len(acc.positive) > 0 {
			resourceWhere += " AND " + spec.col + " IN @" + spec.posBind
			args = append(args, clickhouse.Named(spec.posBind, acc.positive))
		}
		if len(acc.negative) > 0 {
			resourceWhere += " AND " + spec.col + " NOT IN @" + spec.negBind
			args = append(args, clickhouse.Named(spec.negBind, acc.negative))
		}
	}

	return resourceWhere, attrWhere, args
}
