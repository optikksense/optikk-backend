package tracedetail

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	contracts "github.com/observability/observability-backend-go/internal/contracts"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type stubTraceDetailService struct {
	relatedTraces []RelatedTrace
	relatedErr    error
	captured      struct {
		teamID         int64
		serviceName    string
		operationName  string
		startMs        int64
		endMs          int64
		excludeTraceID string
		limit          int
	}
}

func (s *stubTraceDetailService) GetSpanEvents(teamID int64, traceID string) ([]SpanEvent, error) {
	return nil, nil
}

func (s *stubTraceDetailService) GetSpanKindBreakdown(teamID int64, traceID string) ([]SpanKindDuration, error) {
	return nil, nil
}

func (s *stubTraceDetailService) GetCriticalPath(teamID int64, traceID string) ([]CriticalPathSpan, error) {
	return nil, nil
}

func (s *stubTraceDetailService) GetSpanSelfTimes(teamID int64, traceID string) ([]SpanSelfTime, error) {
	return nil, nil
}

func (s *stubTraceDetailService) GetErrorPath(teamID int64, traceID string) ([]ErrorPathSpan, error) {
	return nil, nil
}

func (s *stubTraceDetailService) GetSpanAttributes(teamID int64, traceID, spanID string) (*SpanAttributes, error) {
	return nil, nil
}

func (s *stubTraceDetailService) GetRelatedTraces(teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	s.captured.teamID = teamID
	s.captured.serviceName = serviceName
	s.captured.operationName = operationName
	s.captured.startMs = startMs
	s.captured.endMs = endMs
	s.captured.excludeTraceID = excludeTraceID
	s.captured.limit = limit
	return s.relatedTraces, s.relatedErr
}

func (s *stubTraceDetailService) GetFlamegraphData(teamID int64, traceID string) ([]FlamegraphFrame, error) {
	return nil, nil
}

func newTraceDetailHandlerForTest(service Service) *TraceDetailHandler {
	return &TraceDetailHandler{
		DBTenant: modulecommon.DBTenant{
			GetTenant: func(*gin.Context) contracts.TenantContext {
				return contracts.TenantContext{TeamID: 1}
			},
		},
		Service: service,
	}
}

func TestGetRelatedTracesReturnsSnakeCaseRows(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	service := &stubTraceDetailService{
		relatedTraces: []RelatedTrace{
			{
				TraceID:       "trace-2",
				SpanID:        "span-2",
				OperationName: "POST /api/v1/orders",
				ServiceName:   "checkout-service",
				DurationMs:    210,
				Status:        "OK",
				StartTime:     time.Unix(1_700_000_000, 0).UTC(),
			},
		},
	}

	handler := newTraceDetailHandlerForTest(service)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/traces/trace-1/related?service=checkout-service&operation=POST%20%2Fapi%2Fv1%2Forders&startTime=1000&endTime=2000&limit=5",
		nil,
	)
	ctx.Request = req
	ctx.Params = gin.Params{{Key: "traceId", Value: "trace-1"}}

	handler.GetRelatedTraces(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", recorder.Code)
	}

	var response struct {
		Success bool `json:"success"`
		Data    []struct {
			TraceID       string  `json:"trace_id"`
			SpanID        string  `json:"span_id"`
			OperationName string  `json:"operation_name"`
			ServiceName   string  `json:"service_name"`
			DurationMs    float64 `json:"duration_ms"`
			Status        string  `json:"status"`
			StartTime     string  `json:"start_time"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !response.Success {
		t.Fatalf("expected success response")
	}
	if len(response.Data) != 1 {
		t.Fatalf("expected 1 related trace, got %d", len(response.Data))
	}
	if response.Data[0].TraceID != "trace-2" || response.Data[0].ServiceName != "checkout-service" {
		t.Fatalf("unexpected response row: %+v", response.Data[0])
	}
	if service.captured.excludeTraceID != "trace-1" {
		t.Fatalf("expected exclude trace id trace-1, got %q", service.captured.excludeTraceID)
	}
	if service.captured.limit != 5 {
		t.Fatalf("expected limit 5, got %d", service.captured.limit)
	}
}

func TestGetRelatedTracesValidatesRequiredFields(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	handler := newTraceDetailHandlerForTest(&stubTraceDetailService{})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/traces/trace-1/related?startTime=1000&endTime=2000", nil)
	ctx.Request = req
	ctx.Params = gin.Params{{Key: "traceId", Value: "trace-1"}}

	handler.GetRelatedTraces(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", recorder.Code)
	}
}

func TestGetRelatedTracesReturnsInternalErrorOnServiceFailure(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	handler := newTraceDetailHandlerForTest(&stubTraceDetailService{relatedErr: errors.New("boom")})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/traces/trace-1/related?service=checkout-service&operation=POST%20%2Fapi%2Fv1%2Forders&startTime=1000&endTime=2000",
		nil,
	)
	ctx.Request = req
	ctx.Params = gin.Params{{Key: "traceId", Value: "trace-1"}}

	handler.GetRelatedTraces(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", recorder.Code)
	}
}
