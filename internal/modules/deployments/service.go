package deployments

import (
	"context"
	"math"
)

const maxImpactDeployments = 10

// Service orchestrates deployment detection and impact.
type Service interface {
	ListDeployments(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (ListDeploymentsResponse, error)
	GetVersionTraffic(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) ([]VersionTrafficPoint, error)
	GetDeploymentImpact(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (DeploymentImpactResponse, error)
	GetActiveVersion(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (ActiveVersionResponse, error)
}

type deploymentService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &deploymentService{repo: repo}
}

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

func windowMetrics(ctx context.Context, repo Repository, teamID int64, serviceName string, startMs, endMs int64) (ImpactWindowMetrics, error) {
	if endMs <= startMs {
		return ImpactWindowMetrics{}, nil
	}
	row, err := repo.GetImpactWindow(ctx, teamID, serviceName, startMs, endMs)
	if err != nil {
		return ImpactWindowMetrics{}, err
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
			after, err := windowMetrics(ctx, s.repo, teamID, serviceName, r.FirstSeen.UnixMilli(), afterEnd)
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

		before, err := windowMetrics(ctx, s.repo, teamID, serviceName, beforeStart, beforeEnd)
		if err != nil {
			return DeploymentImpactResponse{}, err
		}
		after, err := windowMetrics(ctx, s.repo, teamID, serviceName, afterStart, afterEnd)
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
