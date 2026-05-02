// Package filter owns the typed log-filter shape, validation, and the
// where-clause emitter every logs reader (explorer, log_facets,
// log_trends) shares. Mirrors internal/modules/metrics/filter.
//
// The repo-side BuildClauses returns (where, args) ready to
// splice into a query of shape:
//
//	SELECT … FROM observability.logs
//	PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
//	WHERE timestamp BETWEEN @start AND @end <where>
package filter

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000

// Filters is the typed filter shape every logs reader consumes. Decoded
// straight off the wire — frontend sends each dimension as a typed array,
// and the handler populates TeamID / StartMs / EndMs from session + the
// top-level request fields.
type Filters struct {
	TeamID  int64 `json:"-"`
	StartMs int64 `json:"-"`
	EndMs   int64 `json:"-"`

	Services     []string `json:"services,omitempty"`
	Hosts        []string `json:"hosts,omitempty"`
	Pods         []string `json:"pods,omitempty"`
	Containers   []string `json:"containers,omitempty"`
	Environments []string `json:"environments,omitempty"`
	Severities   []string `json:"severities,omitempty"`

	TraceID    string `json:"traceId,omitempty"`
	SpanID     string `json:"spanId,omitempty"`
	Search     string `json:"search,omitempty"`
	SearchMode string `json:"searchMode,omitempty"`

	ExcludeServices   []string `json:"excludeServices,omitempty"`
	ExcludeHosts      []string `json:"excludeHosts,omitempty"`
	ExcludeSeverities []string `json:"excludeSeverities,omitempty"`

	Attributes []AttrFilter `json:"attributes,omitempty"`
}

// AttrFilter is a single predicate over `attributes_string[key]`.
// Op ∈ {"eq" (default), "neq", "contains", "regex"}.
type AttrFilter struct {
	Key   string `json:"key"`
	Op    string `json:"op,omitempty"`
	Value string `json:"value"`
}

// Validate clamps the time range to ≤ 30 days, defaults SearchMode to
// "ngram", and rejects missing time bounds. Mutates the receiver in place
// because callers always want the validated/clamped values back.
func (f *Filters) Validate() error {
	if f.EndMs <= 0 {
		f.EndMs = time.Now().UnixMilli()
	}
	if f.StartMs <= 0 {
		return errors.New("filters: startTime is required")
	}
	if f.EndMs <= f.StartMs {
		return errors.New("filters: endTime must be after startTime")
	}
	if (f.EndMs - f.StartMs) > maxTimeRangeMs {
		f.StartMs = f.EndMs - maxTimeRangeMs
	}
	if strings.TrimSpace(f.SearchMode) == "" {
		f.SearchMode = "ngram"
	}
	return nil
}

// BuildClauses turns Filters into (where, args).
// Stable bind names so identical predicate combinations
// produce byte-identical SQL — CH plan cache can hit.
func BuildClauses(f Filters) (resourceWhere, where string, args []any) {
	args = []any{
		clickhouse.Named("teamID", uint32(f.TeamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.BucketStart(f.StartMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(f.EndMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("start", time.UnixMilli(f.StartMs)),
		clickhouse.Named("end", time.UnixMilli(f.EndMs)),
	}

	if len(f.Services) > 0 {
		resourceWhere += ` AND service IN @services`
		args = append(args, clickhouse.Named("services", f.Services))
	}
	if len(f.ExcludeServices) > 0 {
		resourceWhere += ` AND service NOT IN @excServices`
		args = append(args, clickhouse.Named("excServices", f.ExcludeServices))
	}
	if len(f.Hosts) > 0 {
		resourceWhere += ` AND host IN @hosts`
		args = append(args, clickhouse.Named("hosts", f.Hosts))
	}
	if len(f.ExcludeHosts) > 0 {
		resourceWhere += ` AND host NOT IN @excHosts`
		args = append(args, clickhouse.Named("excHosts", f.ExcludeHosts))
	}
	if len(f.Pods) > 0 {
		resourceWhere += ` AND pod IN @pods`
		args = append(args, clickhouse.Named("pods", f.Pods))
	}
	if len(f.Containers) > 0 {
		resourceWhere += ` AND container IN @containers`
		args = append(args, clickhouse.Named("containers", f.Containers))
	}
	if len(f.Environments) > 0 {
		resourceWhere += ` AND environment IN @environments`
		args = append(args, clickhouse.Named("environments", f.Environments))
	}

	if len(f.Severities) > 0 {
		where += ` AND severity_text IN @severities`
		args = append(args, clickhouse.Named("severities", f.Severities))
	}
	if len(f.ExcludeSeverities) > 0 {
		where += ` AND severity_text NOT IN @excSeverities`
		args = append(args, clickhouse.Named("excSeverities", f.ExcludeSeverities))
	}
	if f.TraceID != "" {
		where += ` AND trace_id = @traceID`
		args = append(args, clickhouse.Named("traceID", f.TraceID))
	}
	if f.SpanID != "" {
		where += ` AND span_id = @spanID`
		args = append(args, clickhouse.Named("spanID", f.SpanID))
	}
	if f.Search != "" {
		// hasTokenCaseInsensitive consults the idx_body_tokens tokenbf_v1
		// skip-index defined on observability.logs (see db/clickhouse/02_logs.sql)
		// — granule pruning beats per-row lower(body).
		if f.SearchMode == "exact" {
			where += ` AND positionCaseInsensitive(body, @search) > 0`
		} else {
			where += ` AND hasTokenCaseInsensitive(body, @search)`
		}
		args = append(args, clickhouse.Named("search", f.Search))
	}
	for i, af := range f.Attributes {
		idx := strconv.Itoa(i)
		kName := "akey_" + idx
		vName := "aval_" + idx
		switch af.Op {
		case "neq":
			where += ` AND attributes_string[@` + kName + `] != @` + vName
		case "contains":
			where += ` AND positionCaseInsensitive(attributes_string[@` + kName + `], @` + vName + `) > 0`
		case "regex":
			where += ` AND match(attributes_string[@` + kName + `], @` + vName + `)`
		default:
			where += ` AND attributes_string[@` + kName + `] = @` + vName
		}
		args = append(args, clickhouse.Named(kName, af.Key), clickhouse.Named(vName, af.Value))
	}
	return resourceWhere, where, args
}
