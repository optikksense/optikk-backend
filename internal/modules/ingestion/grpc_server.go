package telemetry

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// GRPCServerConfig holds configuration for the OTLP gRPC server.
type GRPCServerConfig struct {
	Port             string // default "4317"
	MaxRecvMsgSizeMB int    // default 16
	EnableReflection bool   // default true
}

// OTLPGRPCServer implements the three OTel Collector gRPC service interfaces:
//   - TraceService/Export
//   - MetricsService/Export
//   - LogsService/Export
//
// It translates incoming proto requests directly to domain records and
// forwards them to the shared Ingester interface.
type OTLPGRPCServer struct {
	coltracepb.UnimplementedTraceServiceServer
	colmetricspb.UnimplementedMetricsServiceServer
	collogspb.UnimplementedLogsServiceServer

	ingester  Ingester
	spanCache *SpanCache
	onIngest  IngestCallback
}

// NewOTLPGRPCServer creates the OTLP service handler.
func NewOTLPGRPCServer(ingester Ingester, spanCache *SpanCache) *OTLPGRPCServer {
	return &OTLPGRPCServer{
		ingester:  ingester,
		spanCache: spanCache,
	}
}

// SetOnIngest registers a callback that fires after each successful ingest.
func (s *OTLPGRPCServer) SetOnIngest(cb IngestCallback) {
	s.onIngest = cb
}

// Export handles the TraceService/Export RPC.
func (s *OTLPGRPCServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	teamUUID, ok := TeamUUIDFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing team context")
	}

	spans := TranslateProtoSpans(teamUUID, req, s.spanCache)
	if len(spans) == 0 {
		return &coltracepb.ExportTraceServiceResponse{}, nil
	}

	result, err := s.ingester.IngestSpans(ctx, spans)
	if err != nil && (result == nil || result.AcceptedCount == 0) {
		log.Printf("grpc: failed to ingest %d spans: %v", len(spans), err)
		return nil, fmt.Errorf("failed to ingest spans: %w", err)
	}

	if s.onIngest != nil {
		accepted := len(spans)
		if result != nil {
			accepted = result.AcceptedCount
		}
		if accepted > 0 {
			s.onIngest(teamUUID, "spans", accepted)
		}
	}

	resp := &coltracepb.ExportTraceServiceResponse{}
	if result != nil && result.RejectedCount > 0 {
		resp.PartialSuccess = &coltracepb.ExportTracePartialSuccess{
			RejectedSpans: int64(result.RejectedCount),
			ErrorMessage:  result.ErrorMessage,
		}
	}
	return resp, nil
}

// Export handles the MetricsService/Export RPC.
// The method name is the same as TraceService but on a different receiver type
// registered to a different gRPC service. Go's method resolution handles this.
func (s *OTLPGRPCServer) exportMetrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	teamUUID, ok := TeamUUIDFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing team context")
	}

	metrics := TranslateProtoMetrics(teamUUID, req)
	if len(metrics) == 0 {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}

	result, err := s.ingester.IngestMetrics(ctx, metrics)
	if err != nil && (result == nil || result.AcceptedCount == 0) {
		log.Printf("grpc: failed to ingest %d metrics: %v", len(metrics), err)
		return nil, fmt.Errorf("failed to ingest metrics: %w", err)
	}

	if s.onIngest != nil {
		accepted := len(metrics)
		if result != nil {
			accepted = result.AcceptedCount
		}
		if accepted > 0 {
			s.onIngest(teamUUID, "metrics", accepted)
		}
	}

	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if result != nil && result.RejectedCount > 0 {
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: int64(result.RejectedCount),
			ErrorMessage:       result.ErrorMessage,
		}
	}
	return resp, nil
}

// exportLogs handles the LogsService/Export RPC.
func (s *OTLPGRPCServer) exportLogs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	teamUUID, ok := TeamUUIDFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("missing team context")
	}

	logs := TranslateProtoLogs(teamUUID, req)
	if len(logs) == 0 {
		return &collogspb.ExportLogsServiceResponse{}, nil
	}

	result, err := s.ingester.IngestLogs(ctx, logs)
	if err != nil && (result == nil || result.AcceptedCount == 0) {
		log.Printf("grpc: failed to ingest %d logs: %v", len(logs), err)
		return nil, fmt.Errorf("failed to ingest logs: %w", err)
	}

	if s.onIngest != nil {
		accepted := len(logs)
		if result != nil {
			accepted = result.AcceptedCount
		}
		if accepted > 0 {
			s.onIngest(teamUUID, "logs", accepted)
		}
	}

	resp := &collogspb.ExportLogsServiceResponse{}
	if result != nil && result.RejectedCount > 0 {
		resp.PartialSuccess = &collogspb.ExportLogsPartialSuccess{
			RejectedLogRecords: int64(result.RejectedCount),
			ErrorMessage:       result.ErrorMessage,
		}
	}
	return resp, nil
}

// StartGRPCServer creates, configures and starts the gRPC server.
// It returns the *grpc.Server for lifecycle management and the OTLPGRPCServer
// for callback registration.
func StartGRPCServer(
	cfg GRPCServerConfig,
	ingester Ingester,
	spanCache *SpanCache,
	mysql *sql.DB,
) (*grpc.Server, *OTLPGRPCServer, error) {
	if cfg.Port == "" {
		cfg.Port = "4317"
	}
	maxRecvSize := cfg.MaxRecvMsgSizeMB
	if maxRecvSize <= 0 {
		maxRecvSize = 16
	}

	// Create gRPC server with auth interceptor and size limits.
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxRecvSize*1024*1024),
		grpc.UnaryInterceptor(GRPCAuthInterceptor(mysql)),
	)

	// Register OTLP services.
	otlpServer := NewOTLPGRPCServer(ingester, spanCache)
	coltracepb.RegisterTraceServiceServer(srv, otlpServer)

	// For metrics and logs, we need wrapper types since Go doesn't allow
	// the same method name on the same struct for different interfaces.
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsServiceAdapter{s: otlpServer})
	collogspb.RegisterLogsServiceServer(srv, &logsServiceAdapter{s: otlpServer})

	// Register health check service for K8s gRPC probes.
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Register reflection for grpcurl debugging.
	if cfg.EnableReflection {
		reflection.Register(srv)
	}

	// Start listening.
	lis, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		return nil, nil, fmt.Errorf("grpc listen on :%s: %w", cfg.Port, err)
	}

	go func() {
		log.Printf("grpc: OTLP server listening on :%s", cfg.Port)
		if err := srv.Serve(lis); err != nil {
			log.Printf("grpc: server error: %v", err)
		}
	}()

	return srv, otlpServer, nil
}

// ---------------------------------------------------------------------------
// Adapter types to register multiple Export methods on the same handler.
// The OTel proto services all define an `Export` RPC, but Go requires unique
// method names per interface. These thin wrappers delegate to the core server.
// ---------------------------------------------------------------------------

type metricsServiceAdapter struct {
	colmetricspb.UnimplementedMetricsServiceServer
	s *OTLPGRPCServer
}

func (a *metricsServiceAdapter) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	return a.s.exportMetrics(ctx, req)
}

type logsServiceAdapter struct {
	collogspb.UnimplementedLogsServiceServer
	s *OTLPGRPCServer
}

func (a *logsServiceAdapter) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	return a.s.exportLogs(ctx, req)
}
