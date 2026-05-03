// Package filter owns the typed traces-filter shape, validation, and the
// inline-CTE clause emitter every traces reader (explorer, span_query,
// errors) shares. Mirrors internal/modules/metrics/filter and
// internal/modules/logs/filter.
//
// The repo-side BuildClauses returns (resourceWhere, where, args) ready to
// splice into a query of shape:
//
//	WITH active_fps AS (
//	    SELECT DISTINCT fingerprint
//	    FROM observability.spans_resource
//	    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
//	    <resourceWhere>
//	)
//	SELECT … FROM observability.spans
//	PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
//	     AND fingerprint IN active_fps
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

// Filters is the typed filter shape every traces reader consumes. Decoded
// straight off the wire — frontend sends each dimension as a typed array,
// and the handler populates TeamID / StartMs / EndMs from session + the
// top-level request fields.
type Filters struct {
	TeamID  int64 `json:"-"`
	StartMs int64 `json:"-"`
	EndMs   int64 `json:"-"`

	Services      []string `json:"services,omitempty"`
	Operations    []string `json:"operations,omitempty"`
	SpanKinds     []string `json:"spanKinds,omitempty"`
	HTTPMethods   []string `json:"httpMethods,omitempty"`
	HTTPStatuses  []string `json:"httpStatuses,omitempty"`
	Statuses      []string `json:"statuses,omitempty"`
	Environments  []string `json:"environments,omitempty"`
	PeerServices  []string `json:"peerServices,omitempty"`
	TraceID       string   `json:"traceId,omitempty"`
	MinDurationNs int64    `json:"minDurationNs,omitempty"`
	MaxDurationNs int64    `json:"maxDurationNs,omitempty"`
	HasError      *bool    `json:"hasError,omitempty"`

	ExcludeServices []string `json:"excludeServices,omitempty"`
	ExcludeStatuses []string `json:"excludeStatuses,omitempty"`

	Search     string `json:"search,omitempty"`
	SearchMode string `json:"searchMode,omitempty"`

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
// "ngram", and rejects missing time bounds. Mutates the receiver in place.
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

// BuildClauses turns Filters into (resourceWhere, where, args) — same
// emission previously duplicated across explorer, span_query, and errors.
// Stable bind names so identical predicate combinations produce
// byte-identical SQL — CH plan cache can hit.
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
	if len(f.Environments) > 0 {
		resourceWhere += ` AND environment IN @environments`
		args = append(args, clickhouse.Named("environments", f.Environments))
	}

	if len(f.Operations) > 0 {
		where += ` AND name IN @operations`
		args = append(args, clickhouse.Named("operations", f.Operations))
	}
	if len(f.SpanKinds) > 0 {
		where += ` AND kind_string IN @spanKinds`
		args = append(args, clickhouse.Named("spanKinds", f.SpanKinds))
	}
	if len(f.HTTPMethods) > 0 {
		where += ` AND http_method IN @httpMethods`
		args = append(args, clickhouse.Named("httpMethods", f.HTTPMethods))
	}
	if len(f.HTTPStatuses) > 0 {
		where += ` AND toString(response_status_code) IN @httpStatuses`
		args = append(args, clickhouse.Named("httpStatuses", f.HTTPStatuses))
	}
	if len(f.Statuses) > 0 {
		where += ` AND status_code_string IN @statuses`
		args = append(args, clickhouse.Named("statuses", f.Statuses))
	}
	if len(f.PeerServices) > 0 {
		where += ` AND peer_service IN @peerServices`
		args = append(args, clickhouse.Named("peerServices", f.PeerServices))
	}
	if len(f.ExcludeStatuses) > 0 {
		where += ` AND status_code_string NOT IN @excStatuses`
		args = append(args, clickhouse.Named("excStatuses", f.ExcludeStatuses))
	}
	if f.TraceID != "" {
		where += ` AND trace_id = @traceID`
		args = append(args, clickhouse.Named("traceID", f.TraceID))
	}
	if f.MinDurationNs > 0 {
		where += ` AND duration_nano >= @minDur`
		args = append(args, clickhouse.Named("minDur", uint64(f.MinDurationNs))) //nolint:gosec // G115
	}
	if f.MaxDurationNs > 0 {
		where += ` AND duration_nano <= @maxDur`
		args = append(args, clickhouse.Named("maxDur", uint64(f.MaxDurationNs))) //nolint:gosec // G115
	}
	if f.HasError != nil {
		if *f.HasError {
			where += ` AND has_error = 1`
		} else {
			where += ` AND has_error = 0`
		}
	}
	if f.Search != "" {
		if f.SearchMode == "exact" {
			where += ` AND positionCaseInsensitive(name, @search) > 0`
		} else {
			where += ` AND hasToken(lower(name), lower(@search))`
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
