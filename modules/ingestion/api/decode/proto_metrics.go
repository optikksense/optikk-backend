package decode

import (
	"github.com/observability/observability-backend-go/modules/ingestion/model"
	"google.golang.org/protobuf/proto"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// ProtoToMetricsPayload unmarshals a protobuf ExportMetricsServiceRequest into the internal model.
func ProtoToMetricsPayload(body []byte) (model.OTLPMetricsPayload, error) {
	var req colmetricspb.ExportMetricsServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		return model.OTLPMetricsPayload{}, err
	}
	var out model.OTLPMetricsPayload
	for _, rm := range req.GetResourceMetrics() {
		orm := model.OTLPResourceMetrics{
			Resource: protoResource(rm.GetResource()),
		}
		for _, sm := range rm.GetScopeMetrics() {
			osm := model.OTLPScopeMetrics{
				Scope: protoScope(sm.GetScope()),
			}
			for _, m := range sm.GetMetrics() {
				om := model.OTLPMetric{
					Name:        m.GetName(),
					Description: m.GetDescription(),
					Unit:        m.GetUnit(),
				}
				switch d := m.GetData().(type) {
				case *metricspb.Metric_Gauge:
					om.Gauge = &model.OTLPGauge{}
					for _, dp := range d.Gauge.GetDataPoints() {
						om.Gauge.DataPoints = append(om.Gauge.DataPoints, protoNumberDP(dp))
					}
				case *metricspb.Metric_Sum:
					om.Sum = &model.OTLPSum{
						AggregationTemporality: int(d.Sum.GetAggregationTemporality()),
						IsMonotonic:            d.Sum.GetIsMonotonic(),
					}
					for _, dp := range d.Sum.GetDataPoints() {
						om.Sum.DataPoints = append(om.Sum.DataPoints, protoNumberDP(dp))
					}
				case *metricspb.Metric_Histogram:
					om.Histogram = &model.OTLPHistogram{
						AggregationTemporality: int(d.Histogram.GetAggregationTemporality()),
					}
					for _, dp := range d.Histogram.GetDataPoints() {
						om.Histogram.DataPoints = append(om.Histogram.DataPoints, protoHistogramDP(dp))
					}
				}
				osm.Metrics = append(osm.Metrics, om)
			}
			orm.ScopeMetrics = append(orm.ScopeMetrics, osm)
		}
		out.ResourceMetrics = append(out.ResourceMetrics, orm)
	}
	return out, nil
}
