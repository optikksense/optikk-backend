package tracedetail

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_paths"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_shape"
	"golang.org/x/sync/errgroup"
)

// BundleResponse wraps the five always-on trace-detail reads the FE fires on
// page mount. Handler fans them out in parallel so the FE spends one tail
// latency instead of five.
type BundleResponse struct {
	Spans             []SpanListItem                 `json:"spans"`
	Logs              []TraceLog                     `json:"logs"`
	CriticalPath     []trace_paths.CriticalPathSpan `json:"critical_path"`
	ErrorPath        []trace_paths.ErrorPathSpan    `json:"error_path"`
	SpanKindBreakdown []trace_shape.SpanKindDuration `json:"span_kind_breakdown"`
}

// BundleService assembles BundleResponse for GET /traces/:id/bundle.
type BundleService struct {
	traceDetail Service
	spans       TraceSpansService
	paths       trace_paths.Service
	shape       trace_shape.Service
}

func NewBundleService(traceDetail Service, spans TraceSpansService, paths trace_paths.Service, shape trace_shape.Service) *BundleService {
	return &BundleService{traceDetail: traceDetail, spans: spans, paths: paths, shape: shape}
}

// Bundle fans out the five reads via errgroup. Any single failure aborts the
// whole bundle — the FE falls back to individual endpoints in that case.
func (b *BundleService) Bundle(ctx context.Context, teamID int64, traceID string) (BundleResponse, error) {
	var out BundleResponse
	g, gctx := errgroup.WithContext(ctx)
	g.Go(b.spansJob(gctx, teamID, traceID, &out))
	g.Go(b.logsJob(gctx, teamID, traceID, &out))
	g.Go(b.criticalJob(gctx, teamID, traceID, &out))
	g.Go(b.errorJob(gctx, teamID, traceID, &out))
	g.Go(b.kindJob(gctx, teamID, traceID, &out))
	if err := g.Wait(); err != nil {
		return BundleResponse{}, err
	}
	return out, nil
}

func (b *BundleService) spansJob(ctx context.Context, teamID int64, traceID string, out *BundleResponse) func() error {
	return func() error {
		rows, err := b.spans.ListByTrace(ctx, teamID, traceID)
		if err != nil {
			return err
		}
		out.Spans = rows
		return nil
	}
}

func (b *BundleService) logsJob(ctx context.Context, teamID int64, traceID string, out *BundleResponse) func() error {
	return func() error {
		resp, err := b.traceDetail.GetTraceLogs(ctx, teamID, traceID)
		if err != nil || resp == nil {
			return err
		}
		out.Logs = resp.Logs
		return nil
	}
}

func (b *BundleService) criticalJob(ctx context.Context, teamID int64, traceID string, out *BundleResponse) func() error {
	return func() error {
		rows, err := b.paths.GetCriticalPath(ctx, teamID, traceID)
		if err != nil {
			return err
		}
		out.CriticalPath = rows
		return nil
	}
}

func (b *BundleService) errorJob(ctx context.Context, teamID int64, traceID string, out *BundleResponse) func() error {
	return func() error {
		rows, err := b.paths.GetErrorPath(ctx, teamID, traceID)
		if err != nil {
			return err
		}
		out.ErrorPath = rows
		return nil
	}
}

func (b *BundleService) kindJob(ctx context.Context, teamID int64, traceID string, out *BundleResponse) func() error {
	return func() error {
		rows, err := b.shape.GetSpanKindBreakdown(ctx, teamID, traceID)
		if err != nil {
			return err
		}
		out.SpanKindBreakdown = rows
		return nil
	}
}
