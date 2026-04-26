package rollup

// Family constants name every rollup in db/clickhouse/. Readers pass them
// to For; the package appends the tier suffix ("_1m" /
// "_5m" / "_1h") and the "observability." database qualifier to form a
// fully-qualified table name.
//
// Adding a new rollup family is a two-step change: write the DDL (including
// all three tiers) under db/clickhouse/, then add a constant here. Prefer
// extending an existing family over creating a new one — see README.md
// principles.
const (
	FamilySpansRED      = "spans_red"
	FamilySpansKind     = "spans_kind"
	FamilySpansHost     = "spans_host"
	FamilySpansLatency  = "spans_latency"
	FamilySpansTopology = "spans_topology"
	FamilySpansPeer     = "spans_peer"
	FamilySpansDeploys  = "spans_deploys"
	FamilySpansVersion  = "spans_version"
	FamilySpansErrors   = "spans_errors"

	FamilyMetricsGauges         = "metrics_gauges"
	FamilyMetricsGaugesByStatus = "metrics_gauges_by_status"
	FamilyMetricsHist           = "metrics_hist"
	FamilyMetricsK8s            = "metrics_k8s"

	FamilyDBSaturation = "db_saturation"
	FamilyMessaging    = "messaging"

	FamilyLogsVolume   = "logs_volume"
	FamilyLogsFacets   = "logs_facets"
	FamilyTracesFacets = "traces_facets"

	// Narrow MVs — pre-aggregated from parent rollups for endpoint-specific
	// reads. See db/clickhouse/31–36.
	FamilyInfraResourceByService = "infra_resource_by_service"
	FamilySLOBurn                = "slo_burn"
	FamilyDBConnPool             = "db_conn_pool"
	FamilyDBOps                  = "db_ops"
	FamilyDBSummary              = "db_summary"
	FamilyKafkaSummary           = "kafka_summary"
	FamilySpansNode              = "spans_node"
	FamilyDBCollections          = "db_collections"
)

// AllFamilies enumerates every known rollup family. Used by the cardinality
// poller and by ops tooling that needs to iterate every rollup.
var AllFamilies = []string{
	FamilySpansRED, FamilySpansKind, FamilySpansHost, FamilySpansLatency,
	FamilySpansTopology, FamilySpansPeer, FamilySpansDeploys,
	FamilySpansVersion, FamilySpansErrors,
	FamilyMetricsGauges, FamilyMetricsGaugesByStatus, FamilyMetricsHist, FamilyMetricsK8s,
	FamilyDBSaturation, FamilyMessaging,
	FamilyLogsVolume, FamilyLogsFacets, FamilyTracesFacets,
	FamilyInfraResourceByService, FamilySLOBurn, FamilyDBConnPool,
	FamilyDBOps, FamilyDBSummary, FamilyKafkaSummary,
	FamilySpansNode, FamilyDBCollections,
}
