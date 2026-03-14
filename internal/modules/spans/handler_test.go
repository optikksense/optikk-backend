package traces

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	contracts "github.com/observability/observability-backend-go/internal/contracts"
)

type stubTraceRepository struct {
	traces   []Trace
	summary  TraceSummary
	hasMore  bool
	keysetErr error
}

func (s *stubTraceRepository) GetTracesKeyset(ctx context.Context, f TraceFilters, limit int, cursor TraceCursor) ([]Trace, TraceSummary, bool, error) {
	return s.traces, s.summary, s.hasMore, s.keysetErr
}

func (s *stubTraceRepository) GetTraces(ctx context.Context, f TraceFilters, limit, offset int) ([]Trace, int64, TraceSummary, error) {
	return nil, 0, TraceSummary{}, nil
}

func (s *stubTraceRepository) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]Span, error) {
	return nil, nil
}

func (s *stubTraceRepository) GetSpanTree(ctx context.Context, teamID int64, spanID string) ([]Span, error) {
	return nil, nil
}

func (s *stubTraceRepository) GetServiceDependencies(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceDependency, error) {
	return nil, nil
}

func (s *stubTraceRepository) GetOperationAggregation(ctx context.Context, f TraceFilters, limit int) ([]TraceOperationRow, error) {
	return nil, nil
}

func (s *stubTraceRepository) GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	return nil, nil
}

func (s *stubTraceRepository) GetErrorTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ErrorTimeSeries, error) {
	return nil, nil
}

func (s *stubTraceRepository) GetLatencyHistogram(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) ([]LatencyHistogramBucket, error) {
	return nil, nil
}

func (s *stubTraceRepository) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]LatencyHeatmapPoint, error) {
	return nil, nil
}

func TestGetTracesKeysetIncludesTotal(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	repo := &stubTraceRepository{
		traces: []Trace{
			{
				SpanID:        "span-1",
				TraceID:       "trace-1",
				ServiceName:   "checkout-service",
				OperationName: "POST /orders",
				StartTime:     time.Unix(0, 0).UTC(),
				EndTime:       time.Unix(0, 180_000_000).UTC(),
				DurationMs:    180,
				Status:        "OK",
				HTTPMethod:    "POST",
				HTTPStatusCode: 201,
			},
		},
		summary: TraceSummary{
			TotalTraces: 2,
			ErrorTraces: 0,
			P95Duration: 180,
			P99Duration: 180,
		},
	}

	handler := NewHandler(func(c *gin.Context) contracts.TenantContext {
		return contracts.TenantContext{TeamID: 1}
	}, repo)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/traces?startTime=1&endTime=2&offset=0&limit=5", nil)
	ctx.Request = req

	handler.GetTraces(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", recorder.Code)
	}

	var response struct {
		Success bool `json:"success"`
		Data struct {
			Total float64 `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !response.Success {
		t.Fatalf("expected success response")
	}
	if response.Data.Total != 2 {
		t.Fatalf("expected total to be 2, got %v", response.Data.Total)
	}
}
