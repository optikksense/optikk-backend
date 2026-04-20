package deployments

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
)

const maxImpactDeployments = 10
const maxCompareErrors = 8
const maxCompareEndpoints = 12

var ErrDeploymentNotFound = errors.New("deployment not found")

// Service orchestrates deployment detection and impact.
type Service interface {
	ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (ListDeploymentsResponse, error)
	GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]ServiceLatestDeployment, error)
	GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error)
	GetDeploymentImpact(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (DeploymentImpactResponse, error)
	GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (ActiveVersionResponse, error)
	GetDeploymentCompare(ctx context.Context, teamID int64, serviceName, version, environment string, deployedAtMs int64) (DeploymentCompareResponse, error)
}

type deploymentService struct {
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) Service {
	return &deploymentService{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

func (s *deploymentService) ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (ListDeploymentsResponse, error) {
	rows, err := s.repo.ListDeployments(ctx, teamID, serviceName, startMs, endMs)
	if err != nil {
		return ListDeploymentsResponse{}, err
	}
	active, err := s.repo.GetActiveVersion(ctx, teamID, serviceName, startMs, endMs)
	if err != nil {
		return ListDeploymentsResponse{}, err
	}

	out := make([]Deployment, 0, len(rows))
	for _, r := range rows {
		d := Deployment{
			Version:     r.Version,
			Environment: r.Environment,
			FirstSeen:   r.FirstSeen,
			LastSeen:    r.LastSeen,
			SpanCount:   r.SpanCount,
			IsActive:    r.Version == active.Version && r.Environment == active.Environment,
		}
		out = append(out, d)
	}

	return ListDeploymentsResponse{
		Deployments:       out,
		Total:             len(out),
		ActiveVersion:     active.Version,
		ActiveEnvironment: active.Environment,
	}, nil
}

func toServiceLatestDeployment(row deploymentAggRow, isActive bool) ServiceLatestDeployment {
	return ServiceLatestDeployment{
		ServiceName:  row.ServiceName,
		Version:      row.Version,
		Environment:  row.Environment,
		DeployedAt:   row.FirstSeen,
		LastSeenAt:   row.LastSeen,
		IsActive:     isActive,
		CommitSHA:    row.CommitSHA,
		CommitAuthor: row.CommitAuthor,
		RepoURL:      row.RepoURL,
		PRURL:        row.PRURL,
	}
}

func (s *deploymentService) GetLatestDeploymentsByService(ctx context.Context, teamID int64) ([]ServiceLatestDeployment, error) {
	rows, err := s.repo.GetLatestDeploymentsByService(ctx, teamID)
	if err != nil {
		return nil, err
	}

	out := make([]ServiceLatestDeployment, 0, len(rows))
	for _, row := range rows {
		out = append(out, toServiceLatestDeployment(row, true))
	}
	return out, nil
}

func (s *deploymentService) GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error) {
	return s.repo.GetVersionTraffic(ctx, teamID, serviceName, startMs, endMs)
}

func (s *deploymentService) GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (ActiveVersionResponse, error) {
	row, err := s.repo.GetActiveVersion(ctx, teamID, serviceName, startMs, endMs)
	if err != nil {
		return ActiveVersionResponse{}, err
	}
	return ActiveVersionResponse{Version: row.Version, Environment: row.Environment}, nil
}

// fillEndpointPercentiles overlays p95/p99 from the SpanLatencyEndpoint sketch
// onto the CH aggregation. CH returns only the traffic/error counts; the
// sketch carries the merged per-(service, operation, endpoint, method) digest.
// Rows with no sketch coverage stay at zero so the delta math in
// buildEndpointRegressions still works — a "no sketch data" window just
// collapses to zero deltas rather than spurious spikes.
func (s *deploymentService) fillEndpointPercentiles(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64, rows []endpointMetricAggRow) {
	if s.sketchQ == nil || len(rows) == 0 {
		return
	}
	pcts, _ := s.sketchQ.Percentiles(ctx, sketch.SpanLatencyEndpoint, teamIDString(teamID), startMs, endMs, 0.95, 0.99)
	for i := range rows {
		dim := sketch.DimSpanEndpoint(serviceName, rows[i].OperationName, rows[i].EndpointName, rows[i].HTTPMethod)
		if v, ok := pcts[dim]; ok && len(v) == 2 {
			rows[i].P95Ms = v[0]
			rows[i].P99Ms = v[1]
		}
	}
}

func (s *deploymentService) windowMetrics(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (ImpactWindowMetrics, error) {
	if endMs <= startMs {
		return ImpactWindowMetrics{}, nil
	}
	row, err := s.repo.GetImpactWindow(ctx, teamID, serviceName, startMs, endMs)
	if err != nil {
		return ImpactWindowMetrics{}, err
	}
	// Percentiles from sketch.Querier — SpanLatencyService for the given
	// service (CH only returns counts).
	if s.sketchQ != nil {
		pcts, _ := s.sketchQ.Percentiles(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, 0.95, 0.99)
		if v, ok := pcts[sketch.DimSpanService(serviceName)]; ok && len(v) == 2 {
			row.P95Ms = v[0]
			row.P99Ms = v[1]
		}
	}
	sec := float64(endMs-startMs) / 1000.0
	if sec <= 0 {
		sec = 1
	}
	var errRate float64
	if row.RequestCount > 0 {
		errRate = float64(row.ErrorCount) * 100.0 / float64(row.RequestCount)
	}
	return ImpactWindowMetrics{
		RequestCount: row.RequestCount,
		ErrorCount:   row.ErrorCount,
		ErrorRate:    errRate,
		P95Ms:        row.P95Ms,
		P99Ms:        row.P99Ms,
		RPS:          float64(row.RequestCount) / sec,
	}, nil
}

func ptrFloat(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	x := v
	return &x
}

func subPtr(a, b float64) *float64 {
	return ptrFloat(a - b)
}

func normalizeWindow(startMs, endMs int64) (int64, int64) {
	if endMs <= startMs {
		return startMs, startMs + 1
	}
	return startMs, endMs
}

func errorGroupID(service, operation, statusMessage string, httpCode int) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d", service, operation, statusMessage, httpCode)))
	return hex.EncodeToString(hash[:8])
}

func errorSeverity(httpStatusCode int, deltaCount int64, afterCount uint64) string {
	switch {
	case httpStatusCode >= 500 || deltaCount >= 10 || afterCount >= 25:
		return "critical"
	case deltaCount > 0 || afterCount > 0:
		return "warning"
	default:
		return "stable"
	}
}

func buildErrorRegressions(beforeRows, afterRows []errorGroupAggRow) []DeploymentCompareErrorRegression {
	type errorKey struct {
		operation string
		status    string
		code      int
	}

	beforeMap := make(map[errorKey]errorGroupAggRow, len(beforeRows))
	for _, row := range beforeRows {
		beforeMap[errorKey{operation: row.OperationName, status: row.StatusMessage, code: row.HTTPStatusCode}] = row
	}

	keys := make(map[errorKey]struct{}, len(beforeRows)+len(afterRows))
	for _, row := range beforeRows {
		keys[errorKey{operation: row.OperationName, status: row.StatusMessage, code: row.HTTPStatusCode}] = struct{}{}
	}
	for _, row := range afterRows {
		keys[errorKey{operation: row.OperationName, status: row.StatusMessage, code: row.HTTPStatusCode}] = struct{}{}
	}

	regressions := make([]DeploymentCompareErrorRegression, 0, len(keys))
	for key := range keys {
		before := beforeMap[key]
		var after errorGroupAggRow
		for _, row := range afterRows {
			if row.OperationName == key.operation && row.StatusMessage == key.status && row.HTTPStatusCode == key.code {
				after = row
				break
			}
		}

		delta := int64(after.ErrorCount) - int64(before.ErrorCount)
		serviceName := after.ServiceName
		if serviceName == "" {
			serviceName = before.ServiceName
		}
		lastOccurrence := after.LastOccurrence
		if lastOccurrence.IsZero() {
			lastOccurrence = before.LastOccurrence
		}
		sampleTraceID := after.SampleTraceID
		if sampleTraceID == "" {
			sampleTraceID = before.SampleTraceID
		}

		regressions = append(regressions, DeploymentCompareErrorRegression{
			GroupID:        errorGroupID(serviceName, key.operation, key.status, key.code),
			OperationName:  key.operation,
			StatusMessage:  key.status,
			HTTPStatusCode: key.code,
			BeforeCount:    before.ErrorCount,
			AfterCount:     after.ErrorCount,
			DeltaCount:     delta,
			LastOccurrence: lastOccurrence,
			SampleTraceID:  sampleTraceID,
			Severity:       errorSeverity(key.code, delta, after.ErrorCount),
		})
	}

	sort.Slice(regressions, func(i, j int) bool {
		if regressions[i].DeltaCount == regressions[j].DeltaCount {
			return regressions[i].AfterCount > regressions[j].AfterCount
		}
		return regressions[i].DeltaCount > regressions[j].DeltaCount
	})
	if len(regressions) > maxCompareErrors {
		regressions = regressions[:maxCompareErrors]
	}
	return regressions
}

func endpointRegressionScore(row DeploymentCompareEndpointRegression) float64 {
	return math.Max(row.P95DeltaMs, 0) + math.Max(row.P99DeltaMs, 0)*0.4 + math.Max(row.ErrorRateDelta, 0)*35 + math.Max(float64(row.RequestDelta), 0)/250
}

func buildEndpointRegressions(beforeRows, afterRows []endpointMetricAggRow) []DeploymentCompareEndpointRegression {
	type endpointKey struct {
		method   string
		endpoint string
		span     string
	}

	beforeMap := make(map[endpointKey]endpointMetricAggRow, len(beforeRows))
	for _, row := range beforeRows {
		beforeMap[endpointKey{method: row.HTTPMethod, endpoint: row.EndpointName, span: row.OperationName}] = row
	}

	keys := make(map[endpointKey]struct{}, len(beforeRows)+len(afterRows))
	for _, row := range beforeRows {
		keys[endpointKey{method: row.HTTPMethod, endpoint: row.EndpointName, span: row.OperationName}] = struct{}{}
	}
	for _, row := range afterRows {
		keys[endpointKey{method: row.HTTPMethod, endpoint: row.EndpointName, span: row.OperationName}] = struct{}{}
	}

	regressions := make([]DeploymentCompareEndpointRegression, 0, len(keys))
	for key := range keys {
		before := beforeMap[key]
		var after endpointMetricAggRow
		for _, row := range afterRows {
			if row.HTTPMethod == key.method && row.EndpointName == key.endpoint && row.OperationName == key.span {
				after = row
				break
			}
		}

		beforeErrorRate := 0.0
		if before.RequestCount > 0 {
			beforeErrorRate = float64(before.ErrorCount) * 100.0 / float64(before.RequestCount)
		}
		afterErrorRate := 0.0
		if after.RequestCount > 0 {
			afterErrorRate = float64(after.ErrorCount) * 100.0 / float64(after.RequestCount)
		}

		regression := DeploymentCompareEndpointRegression{
			EndpointName:    key.endpoint,
			OperationName:   key.span,
			HTTPMethod:      key.method,
			BeforeRequests:  before.RequestCount,
			AfterRequests:   after.RequestCount,
			RequestDelta:    after.RequestCount - before.RequestCount,
			BeforeErrorRate: beforeErrorRate,
			AfterErrorRate:  afterErrorRate,
			ErrorRateDelta:  afterErrorRate - beforeErrorRate,
			BeforeP95Ms:     before.P95Ms,
			AfterP95Ms:      after.P95Ms,
			P95DeltaMs:      after.P95Ms - before.P95Ms,
			BeforeP99Ms:     before.P99Ms,
			AfterP99Ms:      after.P99Ms,
			P99DeltaMs:      after.P99Ms - before.P99Ms,
		}
		regression.RegressionScore = endpointRegressionScore(regression)
		regressions = append(regressions, regression)
	}

	sort.Slice(regressions, func(i, j int) bool {
		if regressions[i].RegressionScore == regressions[j].RegressionScore {
			return regressions[i].AfterRequests > regressions[j].AfterRequests
		}
		return regressions[i].RegressionScore > regressions[j].RegressionScore
	})
	if len(regressions) > maxCompareEndpoints {
		regressions = regressions[:maxCompareEndpoints]
	}
	return regressions
}

func (s *deploymentService) GetDeploymentImpact(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (DeploymentImpactResponse, error) {
	rows, err := s.repo.ListDeployments(ctx, teamID, serviceName, startMs, endMs)
	if err != nil {
		return DeploymentImpactResponse{}, err
	}
	if len(rows) > maxImpactDeployments {
		rows = rows[len(rows)-maxImpactDeployments:]
	}

	impacts := make([]DeploymentImpactRow, 0, len(rows))
	for i := range rows {
		r := rows[i]
		row := DeploymentImpactRow{
			Version:     r.Version,
			Environment: r.Environment,
		}
		if i == 0 {
			row.IsBaseline = true
			// After window for baseline: first_seen to next or endMs
			afterEnd := endMs
			if i+1 < len(rows) {
				afterEnd = rows[i+1].FirstSeen.UnixMilli()
			}
			after, err := s.windowMetrics(ctx, teamID, serviceName, r.FirstSeen.UnixMilli(), afterEnd)
			if err != nil {
				return DeploymentImpactResponse{}, err
			}
			row.ErrorRateAfter = ptrFloat(after.ErrorRate)
			row.P95After = ptrFloat(after.P95Ms)
			row.RPSAfter = ptrFloat(after.RPS)
			impacts = append(impacts, row)
			continue
		}

		beforeStart := rows[i-1].FirstSeen.UnixMilli()
		beforeEnd := r.FirstSeen.UnixMilli()
		afterStart := r.FirstSeen.UnixMilli()
		afterEnd := endMs
		if i+1 < len(rows) {
			afterEnd = rows[i+1].FirstSeen.UnixMilli()
		}

		before, err := s.windowMetrics(ctx, teamID, serviceName, beforeStart, beforeEnd)
		if err != nil {
			return DeploymentImpactResponse{}, err
		}
		after, err := s.windowMetrics(ctx, teamID, serviceName, afterStart, afterEnd)
		if err != nil {
			return DeploymentImpactResponse{}, err
		}

		row.ErrorRateBefore = ptrFloat(before.ErrorRate)
		row.ErrorRateAfter = ptrFloat(after.ErrorRate)
		row.ErrorRateDelta = subPtr(after.ErrorRate, before.ErrorRate)

		row.P95Before = ptrFloat(before.P95Ms)
		row.P95After = ptrFloat(after.P95Ms)
		row.P95Delta = subPtr(after.P95Ms, before.P95Ms)

		row.RPSBefore = ptrFloat(before.RPS)
		row.RPSAfter = ptrFloat(after.RPS)
		row.RPSDelta = subPtr(after.RPS, before.RPS)

		impacts = append(impacts, row)
	}

	return DeploymentImpactResponse{Impacts: impacts}, nil
}

func (s *deploymentService) GetDeploymentCompare(ctx context.Context, teamID int64, serviceName, version, environment string, deployedAtMs int64) (DeploymentCompareResponse, error) {
	rows, err := s.repo.ListServiceDeployments(ctx, teamID, serviceName)
	if err != nil {
		return DeploymentCompareResponse{}, err
	}
	if len(rows) == 0 {
		return DeploymentCompareResponse{}, nil
	}

	selectedIndex := -1
	for i, row := range rows {
		if row.Version == version && row.Environment == environment && row.FirstSeen.UnixMilli() == deployedAtMs {
			selectedIndex = i
			break
		}
	}
	if selectedIndex == -1 {
		return DeploymentCompareResponse{}, ErrDeploymentNotFound
	}

	selected := rows[selectedIndex]
	afterStart, afterEnd := normalizeWindow(selected.FirstSeen.UnixMilli(), selected.LastSeen.UnixMilli())
	if selectedIndex+1 < len(rows) {
		afterStart, afterEnd = normalizeWindow(selected.FirstSeen.UnixMilli(), rows[selectedIndex+1].FirstSeen.UnixMilli())
	}

	afterMetrics, err := s.windowMetrics(ctx, teamID, serviceName, afterStart, afterEnd)
	if err != nil {
		return DeploymentCompareResponse{}, err
	}

	var beforeWindow *DeploymentCompareWindow
	var beforeMetrics *ImpactWindowMetrics
	beforeStart, beforeEnd := int64(0), int64(0)
	if selectedIndex > 0 {
		beforeStart, beforeEnd = normalizeWindow(rows[selectedIndex-1].FirstSeen.UnixMilli(), selected.FirstSeen.UnixMilli())
		beforeWindow = &DeploymentCompareWindow{StartMs: beforeStart, EndMs: beforeEnd}
		metrics, err := s.windowMetrics(ctx, teamID, serviceName, beforeStart, beforeEnd)
		if err != nil {
			return DeploymentCompareResponse{}, err
		}
		beforeMetrics = &metrics
	}

	beforeErrors := []errorGroupAggRow{}
	beforeEndpoints := []endpointMetricAggRow{}
	if beforeWindow != nil {
		beforeErrors, err = s.repo.GetErrorGroupsWindow(ctx, teamID, serviceName, beforeStart, beforeEnd, maxCompareErrors*2)
		if err != nil {
			return DeploymentCompareResponse{}, err
		}
		beforeEndpoints, err = s.repo.GetEndpointMetricsWindow(ctx, teamID, serviceName, beforeStart, beforeEnd, maxCompareEndpoints*2)
		if err != nil {
			return DeploymentCompareResponse{}, err
		}
		s.fillEndpointPercentiles(ctx, teamID, serviceName, beforeStart, beforeEnd, beforeEndpoints)
	}

	afterErrors, err := s.repo.GetErrorGroupsWindow(ctx, teamID, serviceName, afterStart, afterEnd, maxCompareErrors*2)
	if err != nil {
		return DeploymentCompareResponse{}, err
	}
	afterEndpoints, err := s.repo.GetEndpointMetricsWindow(ctx, teamID, serviceName, afterStart, afterEnd, maxCompareEndpoints*2)
	if err != nil {
		return DeploymentCompareResponse{}, err
	}
	s.fillEndpointPercentiles(ctx, teamID, serviceName, afterStart, afterEnd, afterEndpoints)

	timelineStart := afterStart
	if beforeWindow != nil {
		timelineStart = beforeStart
	}

	return DeploymentCompareResponse{
		Deployment:      toServiceLatestDeployment(selected, selectedIndex == len(rows)-1),
		BeforeWindow:    beforeWindow,
		AfterWindow:     DeploymentCompareWindow{StartMs: afterStart, EndMs: afterEnd},
		HasBaseline:     beforeWindow != nil,
		Summary:         DeploymentCompareSummary{Before: beforeMetrics, After: afterMetrics},
		TopErrors:       buildErrorRegressions(beforeErrors, afterErrors),
		TopEndpoints:    buildEndpointRegressions(beforeEndpoints, afterEndpoints),
		TimelineStartMs: timelineStart,
		TimelineEndMs:   afterEnd,
	}, nil
}
