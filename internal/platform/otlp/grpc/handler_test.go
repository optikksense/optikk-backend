package grpc

import (
	"context"
	"database/sql"
	"testing"

	"github.com/observability/observability-backend-go/internal/platform/ingest"
	"github.com/observability/observability-backend-go/internal/platform/otlp/auth"
	"github.com/observability/observability-backend-go/internal/testutil/sqlstub"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestTraceExportEnqueuesResourcesBeforeSpans(t *testing.T) {
	db := openGRPCTestDB(t)
	defer db.Close()

	resourcesQueue := newGRPCTraceQueue(t, db, "observability.resources", []string{"payload"}, 10)
	spansQueue := newGRPCTraceQueue(t, db, "observability.spans", []string{"payload"}, 0)
	defer closeGRPCQueue(t, resourcesQueue)
	defer closeGRPCQueue(t, spansQueue)

	h := NewHandler(auth.NewAuthenticator(db), resourcesQueue, spansQueue, nil, nil)

	_, err := h.TraceServer.Export(traceContext(), testTraceRequest())
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
	if got := resourcesQueue.QueueLen(); got != 1 {
		t.Fatalf("expected resources queue length 1, got %d", got)
	}
	if got := spansQueue.QueueLen(); got != 0 {
		t.Fatalf("expected spans queue length 0, got %d", got)
	}
}

func TestTraceExportStopsWhenResourcesQueueBackpressures(t *testing.T) {
	db := openGRPCTestDB(t)
	defer db.Close()

	resourcesQueue := newGRPCTraceQueue(t, db, "observability.resources", []string{"payload"}, 0)
	spansQueue := newGRPCTraceQueue(t, db, "observability.spans", []string{"payload"}, 10)
	defer closeGRPCQueue(t, resourcesQueue)
	defer closeGRPCQueue(t, spansQueue)

	h := NewHandler(auth.NewAuthenticator(db), resourcesQueue, spansQueue, nil, nil)

	_, err := h.TraceServer.Export(traceContext(), testTraceRequest())
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
	if got := resourcesQueue.QueueLen(); got != 0 {
		t.Fatalf("expected resources queue length 0, got %d", got)
	}
	if got := spansQueue.QueueLen(); got != 0 {
		t.Fatalf("expected spans queue length 0, got %d", got)
	}
}

func TestTraceExportAcceptsWhenBothQueuesSucceed(t *testing.T) {
	db := openGRPCTestDB(t)
	defer db.Close()

	resourcesQueue := newGRPCTraceQueue(t, db, "observability.resources", []string{"payload"}, 10)
	spansQueue := newGRPCTraceQueue(t, db, "observability.spans", []string{"payload"}, 10)
	defer closeGRPCQueue(t, resourcesQueue)
	defer closeGRPCQueue(t, spansQueue)

	h := NewHandler(auth.NewAuthenticator(db), resourcesQueue, spansQueue, nil, nil)

	resp, err := h.TraceServer.Export(traceContext(), testTraceRequest())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if resp == nil {
		t.Fatal("expected response")
	}
	if got := resourcesQueue.QueueLen(); got != 1 {
		t.Fatalf("expected resources queue length 1, got %d", got)
	}
	if got := spansQueue.QueueLen(); got != 1 {
		t.Fatalf("expected spans queue length 1, got %d", got)
	}
}

func openGRPCTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sqlstub.OpenTeamDB(42)
	if err != nil {
		t.Fatalf("open stub db: %v", err)
	}
	return db
}

func newGRPCTraceQueue(t *testing.T, db *sql.DB, table string, columns []string, maxQueueSize int) *ingest.Queue {
	t.Helper()

	return ingest.NewQueue(
		db,
		table,
		columns,
		ingest.WithBatchSize(1000),
		ingest.WithFlushInterval(60_000),
		ingest.WithMaxQueueSize(maxQueueSize),
	)
}

func closeGRPCQueue(t *testing.T, q *ingest.Queue) {
	t.Helper()
	if q == nil {
		return
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close queue: %v", err)
	}
}

func traceContext() context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "test-key"))
}

func testTraceRequest() *tracepb.ExportTraceServiceRequest {
	return &tracepb.ExportTraceServiceRequest{
		ResourceSpans: []*trace.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						protoStringKV("service.name", "checkout"),
						protoStringKV("deployment.environment", "prod"),
					},
				},
				ScopeSpans: []*trace.ScopeSpans{
					{
						Spans: []*trace.Span{
							{
								TraceId:           []byte("trace-1-trace-1-"),
								SpanId:            []byte("span-001"),
								Name:              "GET /checkout",
								Kind:              trace.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: 1710000300000000000,
								EndTimeUnixNano:   1710000305000000000,
								Status:            &trace.Status{Code: trace.Status_STATUS_CODE_UNSET},
							},
						},
					},
				},
			},
		},
	}
}
