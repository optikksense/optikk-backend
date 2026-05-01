package tracedetail

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	trace_logs "github.com/Optikk-Org/optikk-backend/internal/modules/logs/trace_logs"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_paths"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/trace_shape"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *TraceDetailHandler, sh *SpansHandler, bh *BundleHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/traces/:traceId/span-events", h.GetSpanEvents)
	v1.GET("/traces/:traceId/spans/:spanId/attributes", h.GetSpanAttributes)
	v1.GET("/traces/:traceId/spans/:spanId/logs", h.GetSpanLogs)
	v1.GET("/traces/:traceId/related", h.GetRelatedTraces)
	if sh != nil {
		v1.GET("/traces/:traceId/spans", sh.GetTraceSpans)
		v1.GET("/spans/:spanId/tree", sh.GetSpanTree)
	}
	if bh != nil {
		v1.GET("/traces/:traceId/bundle", bh.GetBundle)
	}
}

func NewModule(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) registry.Module {
	module := &traceDetailModule{}
	module.configure(nativeQuerier, getTenant)
	return module
}

type traceDetailModule struct {
	handler       *TraceDetailHandler
	spansHandler  *SpansHandler
	bundleHandler *BundleHandler
}

func (m *traceDetailModule) Name() string { return "traceDetail" }

func (m *traceDetailModule) configure(nativeQuerier clickhouse.Conn, getTenant registry.GetTenantFunc) {
	svc := NewService(NewRepository(nativeQuerier))
	spans := NewTraceSpansService(nativeQuerier)
	m.handler = &TraceDetailHandler{DBTenant: modulecommon.DBTenant{GetTenant: getTenant}, Service: svc}
	m.spansHandler = NewSpansHandler(getTenant, spans)
	// Bundle composes the five always-on reads from sibling trace_* modules
	// (trace_paths, trace_shape) plus the new trace_logs module that owns
	// "all logs for a trace" via the observability.trace_index reverse-projection.
	paths := trace_paths.NewService(trace_paths.NewRepository(nativeQuerier))
	shape := trace_shape.NewService(trace_shape.NewRepository(nativeQuerier))
	traceLogsSvc := trace_logs.NewService(trace_logs.NewRepository(nativeQuerier))
	m.bundleHandler = &BundleHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  NewBundleService(svc, spans, paths, shape, traceLogsSvc),
	}
}

func (m *traceDetailModule) RegisterRoutes(group *gin.RouterGroup) {
	RegisterRoutes(DefaultConfig(), group, m.handler, m.spansHandler, m.bundleHandler)
}
