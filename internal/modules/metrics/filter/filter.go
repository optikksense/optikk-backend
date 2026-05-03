// Package filter owns the typed Filters shape, canonical resource-key
// resolution, and SQL clause emission for the metrics module's read path.
//
// Metrics is an open-dimension explorer — the user can filter on any tag key
// — so Filters keeps a generic Tags []TagFilter list rather than a closed
// set of typed dimensions like logs/traces. BuildClauses partitions that
// list internally into a resource-CTE fragment (canonical resource keys
// against observability.metrics_resource) and a row-WHERE fragment
// (everything else against observability.metrics's per-data-point attributes
// JSON column).
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

// TagFilter is a single filter row (post-handler-normalization). Operator
// values are the SQL forms ("=", "!=", "IN", "NOT IN") — frontend operator
// names ("eq"/"neq"/"in"/"not_in") are mapped before reaching here.
type TagFilter struct {
	Key      string
	Operator string
	Values   []string
}

// validAggregations is the set of aggregation functions the repo supports.
// Kept here so Validate can check up front rather than the repo failing
// halfway through query construction.
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

// ResourceColumn returns the metrics_resource column for a canonical
// resource key. metrics_resource stores these as flat LowCardinality
// columns, so the CTE-side fragment is a simple "<col> IN @<bind>" form.
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

// AttrColumn returns the JSON-path column expression for a non-resource
// tag key against observability.metrics's per-data-point attributes column.
// The key is sanitized against [a-zA-Z0-9._-] before being spliced in.
func AttrColumn(key string) string {
	return "attributes.`" + SanitizeKey(key) + "`::String"
}

// GroupByColumn picks ResourceColumn (when canonical) or AttrColumn for the
// SELECT/GROUP BY of a user-supplied groupBy key. For canonical resource
// keys against observability.metrics, the resource value lives under the
// resource JSON column rather than the data-point attributes column.
func GroupByColumn(key string) string {
	if canonical := Canonical(key); canonical != "" {
		return resourceJSONPath(canonical)
	}
	return AttrColumn(key)
}

// resourceJSONPath returns the resource JSON-path column expression for a
// canonical resource key against observability.metrics (raw table, not
// the dictionary). Used by GROUP BY when grouping by a resource dim.
func resourceJSONPath(canonical string) string {
	switch canonical {
	case "service":
		return "resource.`service.name`::String"
	case "host":
		return "resource.`host.name`::String"
	case "environment":
		return "resource.`deployment.environment`::String"
	case "k8s_namespace":
		return "resource.`k8s.namespace.name`::String"
	}
	return ""
}

// SanitizeKey strips characters outside [a-zA-Z0-9._-]. The metrics module
// accepts arbitrary user-supplied tag keys for column expressions; this
// guard prevents SQL injection via the key part. Values always bind via
// clickhouse.Named() and never need this.
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

// BuildClauses partitions f.Tags into (resourceWhere, where, args) ready to
// splice into the canonical metrics query shape (see package doc).
//
// Resource keys collapse into 8 stable bind names (services / excServices /
// hosts / excHosts / environments / excEnvironments / k8sNamespaces /
// excK8sNamespaces) — multiple filter rows on the same canonical key fold
// into one IN/NOT IN clause. Row-side keys emit per-row clauses with
// indexed bind names (mf0, mf1, …).
//
// Identical filter shapes (same set of canonical keys touched, same row-key
// order) produce byte-identical SQL strings — CH's plan cache can hit.
func BuildClauses(f Filters) (resourceWhere, where string, args []any) {
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
			where += " AND " + col + " = @" + bind
			args = append(args, clickhouse.Named(bind, t.Values[0]))
		case "!=":
			where += " AND " + col + " != @" + bind
			args = append(args, clickhouse.Named(bind, t.Values[0]))
		case "IN":
			where += " AND " + col + " IN @" + bind
			args = append(args, clickhouse.Named(bind, t.Values))
		case "NOT IN":
			where += " AND " + col + " NOT IN @" + bind
			args = append(args, clickhouse.Named(bind, t.Values))
		}
	}

	// Emit resource clauses in fixed order so identical filter shapes produce
	// byte-identical SQL even when the input list ordering varies.
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

	return resourceWhere, where, args
}
