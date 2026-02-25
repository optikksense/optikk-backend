package metrics

import modulecommon "github.com/observability/observability-backend-go/internal/modules/common"

// MetricHandler handles metrics, service topology, infrastructure, and dashboard endpoints.
type MetricHandler struct {
	modulecommon.DBTenant
	Repo *Repository
}
