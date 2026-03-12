package database

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository declares all database observability queries.
type Repository interface {
	// Section 1
	GetSummaryStats(teamID int64, startMs, endMs int64, f Filters) (SummaryStats, error)

	// Section 2
	GetDetectedSystems(teamID int64, startMs, endMs int64) ([]DetectedSystem, error)

	// Section 3
	GetLatencyBySystem(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyByOperation(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyByCollection(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyByNamespace(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyByServer(teamID int64, startMs, endMs int64, f Filters) ([]LatencyTimeSeries, error)
	GetLatencyHeatmap(teamID int64, startMs, endMs int64, f Filters) ([]LatencyHeatmapBucket, error)

	// Section 4
	GetOpsBySystem(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error)
	GetOpsByOperation(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error)
	GetOpsByCollection(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error)
	GetReadVsWrite(teamID int64, startMs, endMs int64, f Filters) ([]ReadWritePoint, error)
	GetOpsByNamespace(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error)

	// Section 5
	GetSlowQueryPatterns(teamID int64, startMs, endMs int64, f Filters, limit int) ([]SlowQueryPattern, error)
	GetSlowestCollections(teamID int64, startMs, endMs int64, f Filters) ([]SlowCollectionRow, error)
	GetSlowQueryRate(teamID int64, startMs, endMs int64, f Filters, thresholdMs float64) ([]SlowRatePoint, error)
	GetP99ByQueryText(teamID int64, startMs, endMs int64, f Filters, limit int) ([]P99ByQueryText, error)

	// Section 6
	GetErrorsBySystem(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorsByOperation(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorsByErrorType(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorsByCollection(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorsByResponseStatus(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error)
	GetErrorRatio(teamID int64, startMs, endMs int64, f Filters) ([]ErrorRatioPoint, error)

	// Sections 7 & 8
	GetConnectionCountSeries(teamID int64, startMs, endMs int64) ([]ConnectionCountPoint, error)
	GetConnectionUtilization(teamID int64, startMs, endMs int64) ([]ConnectionUtilPoint, error)
	GetConnectionLimits(teamID int64, startMs, endMs int64) ([]ConnectionLimits, error)
	GetPendingRequests(teamID int64, startMs, endMs int64) ([]PendingRequestsPoint, error)
	GetConnectionTimeoutRate(teamID int64, startMs, endMs int64) ([]ConnectionTimeoutPoint, error)
	GetConnectionWaitTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error)
	GetConnectionCreateTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error)
	GetConnectionUseTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error)

	// Section 9
	GetCollectionLatency(teamID int64, startMs, endMs int64, collection string, f Filters) ([]LatencyTimeSeries, error)
	GetCollectionOps(teamID int64, startMs, endMs int64, collection string, f Filters) ([]OpsTimeSeries, error)
	GetCollectionErrors(teamID int64, startMs, endMs int64, collection string, f Filters) ([]ErrorTimeSeries, error)
	GetCollectionQueryTexts(teamID int64, startMs, endMs int64, collection string, f Filters, limit int) ([]CollectionTopQuery, error)
	GetCollectionReadVsWrite(teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error)

	// Section 10
	GetSystemLatency(teamID int64, startMs, endMs int64, dbSystem string, f Filters) ([]LatencyTimeSeries, error)
	GetSystemOps(teamID int64, startMs, endMs int64, dbSystem string, f Filters) ([]OpsTimeSeries, error)
	GetSystemTopCollectionsByLatency(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error)
	GetSystemTopCollectionsByVolume(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error)
	GetSystemErrors(teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error)
	GetSystemNamespaces(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error)
}

// ClickHouseRepository is the ClickHouse-backed implementation.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository returns a Repository backed by ClickHouse.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

// ---------------------------------------------------------------------------
// Shared filter helper
// ---------------------------------------------------------------------------

// filterClauses builds supplementary WHERE clauses from optional Filters.
// Returns the SQL snippet (starting with " AND") and the bound args.
func filterClauses(f Filters) (string, []any) {
	var sb strings.Builder
	var args []any

	appendIn := func(attr string, values []string) {
		if len(values) == 0 {
			return
		}
		clause, a := dbutil.InClause(values)
		sb.WriteString(fmt.Sprintf(" AND %s IN %s", attrString(attr), clause))
		args = append(args, a...)
	}

	appendIn(AttrDBSystem, f.DBSystem)
	appendIn(AttrDBCollectionName, f.Collection)
	appendIn(AttrDBNamespace, f.Namespace)
	appendIn(AttrServerAddress, f.Server)

	return sb.String(), args
}

// buildArgs prepends the fixed args to the filter args.
func buildArgs(fixed []any, extra []any) []any {
	return append(fixed, extra...)
}
