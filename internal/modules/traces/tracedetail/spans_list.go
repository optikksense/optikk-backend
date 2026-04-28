// spans_list.go absorbs two endpoints originally split out of the deleted
// traces/query module:
//   - GET /traces/:traceId/spans  — flat list of every span in a trace
//   - GET /spans/:spanId/tree    — subtree starting at the given span
//
// Repo-clean: the SQL is pure projection + filter; the subtree-walk happens
// in TraceSpansService (Go-side). Same shape as the rest of tracedetail's
// trace-id-scoped reads — PREWHERE on team_id, traceIDMatchPredicate
// inlined, no fmt.Sprintf, no separate compile pass.
package tracedetail

import (
	"context"
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// SpanListItem is the wire shape returned by both /traces/:traceId/spans and
// /spans/:spanId/tree. Compact on purpose — detail comes from the existing
// GetSpanAttributes endpoint.
type SpanListItem struct {
	SpanID        string  `json:"span_id"        ch:"span_id"`
	ParentSpanID  string  `json:"parent_span_id" ch:"parent_span_id"`
	TraceID       string  `json:"trace_id"       ch:"trace_id"`
	ServiceName   string  `json:"service_name"   ch:"service"`
	OperationName string  `json:"operation_name" ch:"name"`
	KindString    string  `json:"kind"           ch:"kind_string"`
	StatusCode    string  `json:"status_code"    ch:"status_code_string"`
	HasError      bool    `json:"has_error"      ch:"has_error"`
	DurationMs    float64 `json:"duration_ms"    ch:"duration_ms"`
	StartNs       int64   `json:"start_ns"       ch:"start_ns"`
}

// TraceSpansService is the read surface for the two absorbed endpoints.
// Wraps a thin db handle; service-side filterSubtree walks the parent
// chain Go-side after the query runs.
type TraceSpansService interface {
	ListByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error)
	Subtree(ctx context.Context, teamID int64, spanID string) ([]SpanListItem, error)
}

type traceSpansService struct{ db clickhouse.Conn }

// NewTraceSpansService wires a fresh in-module service backed by CH directly;
// kept separate from the primary tracedetail Service to avoid disturbing the
// existing interface surface.
func NewTraceSpansService(db clickhouse.Conn) TraceSpansService {
	return &traceSpansService{db: db}
}

func (s *traceSpansService) ListByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error) {
	const query = `
		SELECT span_id,
		       parent_span_id,
		       trace_id,
		       service,
		       name,
		       kind_string,
		       status_code_string,
		       has_error,
		       duration_nano / 1000000.0          AS duration_ms,
		       toUnixTimestamp64Nano(timestamp)   AS start_ns
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE ` + traceIDMatchPredicate + `
		ORDER BY start_ns ASC
		LIMIT 5000`
	var rows []SpanListItem
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), s.db, "tracedetail.ListByTrace", &rows, query, traceIDArgs(teamID, traceID)...)
}

// Subtree resolves trace_id from span_id in a CTE, then projects every span
// in that trace; filterSubtree (service-side, Go) keeps only descendants
// of the root spanID.
func (s *traceSpansService) Subtree(ctx context.Context, teamID int64, spanID string) ([]SpanListItem, error) {
	const query = `
		WITH start AS (
		    SELECT trace_id
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND span_id = @spanID
		    LIMIT 1
		)
		SELECT span_id,
		       parent_span_id,
		       trace_id,
		       service,
		       name,
		       kind_string,
		       status_code_string,
		       has_error,
		       duration_nano / 1000000.0          AS duration_ms,
		       toUnixTimestamp64Nano(timestamp)   AS start_ns
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE trace_id IN (SELECT trace_id FROM start)
		ORDER BY start_ns ASC
		LIMIT 5000`
	var rows []SpanListItem
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), s.db, "tracedetail.Subtree", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("spanID", spanID),
	); err != nil {
		return nil, err
	}
	return filterSubtree(rows, spanID), nil
}

// filterSubtree walks the flat list and keeps only spans reachable from
// rootID via the parent_span_id → span_id chain. Service-side; the CH query
// can't express transitive closure cleanly without recursive CTEs.
func filterSubtree(rows []SpanListItem, rootID string) []SpanListItem {
	keep := map[string]bool{rootID: true}
	changed := true
	for changed {
		changed = false
		for _, row := range rows {
			if !keep[row.SpanID] && keep[row.ParentSpanID] {
				keep[row.SpanID] = true
				changed = true
			}
		}
	}
	out := make([]SpanListItem, 0, len(rows))
	for _, row := range rows {
		if keep[row.SpanID] {
			out = append(out, row)
		}
	}
	return out
}

// SpansHandler hosts the two new endpoints. Kept separate from the core
// TraceDetailHandler so the existing 9 endpoints' wiring is untouched.
type SpansHandler struct {
	modulecommon.DBTenant
	svc TraceSpansService
}

func NewSpansHandler(getTenant modulecommon.GetTenantFunc, svc TraceSpansService) *SpansHandler {
	return &SpansHandler{DBTenant: modulecommon.DBTenant{GetTenant: getTenant}, svc: svc}
}

func (h *SpansHandler) GetTraceSpans(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	items, err := h.svc.ListByTrace(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list trace spans", err)
		return
	}
	if items == nil {
		items = []SpanListItem{}
	}
	modulecommon.RespondOK(c, gin.H{"spans": items})
}

func (h *SpansHandler) GetSpanTree(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	items, err := h.svc.Subtree(c.Request.Context(), teamID, spanID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list span subtree", err)
		return
	}
	if items == nil {
		items = []SpanListItem{}
	}
	modulecommon.RespondOK(c, gin.H{"spans": items})
}
