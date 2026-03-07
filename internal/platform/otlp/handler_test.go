package otlp

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/platform/ingest"
	"github.com/observability/observability-backend-go/internal/platform/otlp/auth"
	"github.com/observability/observability-backend-go/internal/testutil/sqlstub"
)

func TestExportTracesEnqueuesResourcesBeforeSpans(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openHandlerTestDB(t)
	defer db.Close()

	resourcesQueue := newTraceTestQueue(t, db, "observability.resources", ResourceColumns, 10)
	spansQueue := newTraceTestQueue(t, db, "observability.spans", SpanColumns, 0)
	defer closeQueue(t, resourcesQueue)
	defer closeQueue(t, spansQueue)

	h := NewHandler(auth.NewAuthenticator(db), resourcesQueue, spansQueue, nil, nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/otlp/v1/traces", bytes.NewBufferString(testTracePayloadJSON))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("X-API-Key", "test-key")

	h.ExportTraces(c)

	if got := c.Writer.Status(); got != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when spans queue is full, got %d", got)
	}
	if got := resourcesQueue.QueueLen(); got != 1 {
		t.Fatalf("expected resources queue length 1, got %d", got)
	}
	if got := spansQueue.QueueLen(); got != 0 {
		t.Fatalf("expected spans queue length 0, got %d", got)
	}
}

func TestExportTracesStopsWhenResourcesQueueBackpressures(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openHandlerTestDB(t)
	defer db.Close()

	resourcesQueue := newTraceTestQueue(t, db, "observability.resources", ResourceColumns, 0)
	spansQueue := newTraceTestQueue(t, db, "observability.spans", SpanColumns, 10)
	defer closeQueue(t, resourcesQueue)
	defer closeQueue(t, spansQueue)

	h := NewHandler(auth.NewAuthenticator(db), resourcesQueue, spansQueue, nil, nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/otlp/v1/traces", bytes.NewBufferString(testTracePayloadJSON))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("X-API-Key", "test-key")

	h.ExportTraces(c)

	if got := c.Writer.Status(); got != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when resources queue is full, got %d", got)
	}
	if got := resourcesQueue.QueueLen(); got != 0 {
		t.Fatalf("expected resources queue length 0, got %d", got)
	}
	if got := spansQueue.QueueLen(); got != 0 {
		t.Fatalf("expected spans queue length 0, got %d", got)
	}
}

func TestExportTracesAcceptsWhenBothQueuesSucceed(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openHandlerTestDB(t)
	defer db.Close()

	resourcesQueue := newTraceTestQueue(t, db, "observability.resources", ResourceColumns, 10)
	spansQueue := newTraceTestQueue(t, db, "observability.spans", SpanColumns, 10)
	defer closeQueue(t, resourcesQueue)
	defer closeQueue(t, spansQueue)

	h := NewHandler(auth.NewAuthenticator(db), resourcesQueue, spansQueue, nil, nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/otlp/v1/traces", bytes.NewBufferString(testTracePayloadJSON))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("X-API-Key", "test-key")

	h.ExportTraces(c)

	if got := c.Writer.Status(); got != http.StatusAccepted {
		t.Fatalf("expected 202 accepted, got %d", got)
	}
	if got := resourcesQueue.QueueLen(); got != 1 {
		t.Fatalf("expected resources queue length 1, got %d", got)
	}
	if got := spansQueue.QueueLen(); got != 1 {
		t.Fatalf("expected spans queue length 1, got %d", got)
	}
}

func openHandlerTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sqlstub.OpenTeamDB(42)
	if err != nil {
		t.Fatalf("open stub db: %v", err)
	}
	return db
}

func newTraceTestQueue(t *testing.T, db *sql.DB, table string, columns []string, maxQueueSize int) *ingest.Queue {
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

func closeQueue(t *testing.T, q *ingest.Queue) {
	t.Helper()
	if q == nil {
		return
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close queue: %v", err)
	}
}

const testTracePayloadJSON = `{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "checkout" } },
          { "key": "deployment.environment", "value": { "stringValue": "prod" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "checkout.instrumentation", "version": "1.0.0" },
          "spans": [
            {
              "traceId": "trace-1",
              "spanId": "span-1",
              "name": "GET /checkout",
              "kind": 2,
              "startTimeUnixNano": "1710000300000000000",
              "endTimeUnixNano": "1710000305000000000",
              "status": { "code": 0 }
            }
          ]
        }
      ]
    }
  ]
}`
