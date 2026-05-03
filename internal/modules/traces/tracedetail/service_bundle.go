package tracedetail

import (
	"context"

	trace_logs "github.com/Optikk-Org/optikk-backend/internal/modules/logs/trace_logs"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_paths"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_shape"
	"golang.org/x/sync/errgroup"
)

const bundleLogsLimit = 1000

// BundleResponse wraps the five always-on trace-detail reads the FE fires on
// page mount. Handler fans them out in parallel so the FE spends one tail
// latency instead of five.
type BundleResponse struct {
	Spans             []SpanListItem                 `json:"spans"`
	Logs              []TraceLog                     `json:"logs"`
	CriticalPath      []trace_paths.CriticalPathSpan `json:"critical_path"`
	ErrorPath         []trace_paths.ErrorPathSpan    `json:"error_path"`
	SpanKindBreakdown []trace_shape.SpanKindDuration `json:"span_kind_breakdown"`
}

// BundleService assembles BundleResponse for GET /traces/:id/bundle.
type BundleService struct {
	traceDetail Service
	spans       TraceSpansService
	paths       trace_paths.Service
	shape       trace_shape.Service
	traceLogs   *trace_logs.Service
}

func NewBundleService(traceDetail Service, spans TraceSpansService, paths trace_paths.Service, shape trace_shape.Service, traceLogs *trace_logs.Service) *BundleService {
	return &BundleService{traceDetail: traceDetail, spans: spans, paths: paths, shape: shape, traceLogs: traceLogs}
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
		logs, err := b.traceLogs.GetByTraceID(ctx, teamID, traceID, bundleLogsLimit)
		if err != nil {
			return err
		}
		out.Logs = make([]TraceLog, len(logs))
		for i, l := range logs {
			out.Logs[i] = TraceLog{
				Timestamp:         l.Timestamp,
				ObservedTimestamp: l.ObservedTimestamp,
				SeverityText:      l.SeverityText,
				SeverityNumber:    l.SeverityNumber,
				Body:              l.Body,
				TraceID:           l.TraceID,
				SpanID:            l.SpanID,
				TraceFlags:        l.TraceFlags,
				ServiceName:       l.ServiceName,
				Host:              l.Host,
				Pod:               l.Pod,
				Container:         l.Container,
				Environment:       l.Environment,
				AttributesString:  l.AttributesString,
				AttributesNumber:  l.AttributesNumber,
				AttributesBool:    l.AttributesBool,
				ScopeName:         l.ScopeName,
				ScopeVersion:      l.ScopeVersion,
			}
		}
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
