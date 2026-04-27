// Additions to tracedetail module absorbing two endpoints that used to live
// in the deleted traces/query module:
//   - GET /traces/:traceId/spans — all spans in a trace (tree flat)
//   - GET /spans/:spanId/tree — subtree starting at a span
//
// These stay scoped to tracedetail because they're trace-scoped drill-downs
// that TraceDetailPage / SpanDetailDrawer depend on. Existing 9 endpoints
// are untouched; this file only adds new surface.
package tracedetail

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/traceidmatch"
	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// SpanListItem is the wire shape returned by both /traces/:traceId/spans and
// /spans/:spanId/tree. Compact on purpose — detail comes from the existing
// GetSpanAttributes endpoint.
type SpanListItem struct {
	SpanID		string	`json:"span_id" ch:"span_id"`
	ParentSpanID	string	`json:"parent_span_id" ch:"parent_span_id"`
	TraceID		string	`json:"trace_id" ch:"trace_id"`
	ServiceName	string	`json:"service_name" ch:"service_name"`
	OperationName	string	`json:"operation_name" ch:"name"`
	KindString	string	`json:"kind" ch:"kind_string"`
	StatusCode	string	`json:"status_code" ch:"status_code_string"`
	HasError	bool	`json:"has_error" ch:"has_error"`
	DurationMs	float64	`json:"duration_ms" ch:"duration_ms"`
	StartNs		int64	`json:"start_ns" ch:"start_ns"`
}

// TraceSpansService is the read surface for the two absorbed endpoints.
type TraceSpansService interface {
	ListByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error)
	Subtree(ctx context.Context, teamID int64, spanID string) ([]SpanListItem, error)
}

type spansRepo struct{ db clickhouse.Conn }

// NewTraceSpansService wires a fresh in-module service backed by CH directly;
// kept separate from the primary tracedetail Service to avoid disturbing the
// existing interface surface.
func NewTraceSpansService(db clickhouse.Conn) TraceSpansService	{ return &spansRepo{db: db} }

const (
	spansRawTable		= "observability.signoz_index_v3"
	spansByTraceTable	= "observability.signoz_index_v3"
)

func (r *spansRepo) ListByTrace(ctx context.Context, teamID int64, traceID string) ([]SpanListItem, error) {
	// Phase 7: read from the MV keyed on (team_id, trace_id, span_id) —
	// narrow range scan instead of a bloom-filter guess on the 100M-row spans.
	query := fmt.Sprintf(`
		SELECT span_id, parent_span_id, trace_id, service_name, name,
			kind_string, status_code_string, has_error,
			duration_nano / 1000000.0 AS duration_ms,
			toUnixTimestamp64Nano(timestamp) AS start_ns
		FROM %s
		PREWHERE team_id = @teamID
		WHERE %s
		ORDER BY start_ns ASC
		LIMIT 5000`, spansByTraceTable, traceidmatch.WhereTraceIDMatchesCH("trace_id", "traceID"))
	var rows []SpanListItem
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "tracedetail.ListByTrace", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	)
	return rows, err
}

func (r *spansRepo) Subtree(ctx context.Context, teamID int64, spanID string) ([]SpanListItem, error) {
	query := fmt.Sprintf(`
		WITH start AS (
			SELECT trace_id FROM %s
			WHERE team_id = @teamID AND span_id = @spanID LIMIT 1
		)
		SELECT s.span_id, s.parent_span_id, s.trace_id, s.service_name, s.name,
			s.kind_string, s.status_code_string, s.has_error,
			s.duration_nano / 1000000.0 AS duration_ms,
			toUnixTimestamp64Nano(s.timestamp) AS start_ns
		FROM %s s
		WHERE s.team_id = @teamID AND s.trace_id IN (SELECT trace_id FROM start)
		ORDER BY start_ns ASC
		LIMIT 5000`, spansRawTable, spansRawTable)
	var rows []SpanListItem
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "tracedetail.Subtree", &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("spanID", spanID),
	)
	return filterSubtree(rows, spanID), err
}

// filterSubtree walks the flat list and keeps only spans reachable from root.
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
	svc	TraceSpansService
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
