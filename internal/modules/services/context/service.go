package servicecontext

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	serviceinventory "github.com/Optikk-Org/optikk-backend/internal/modules/services/inventory"
)

const (
	monitorStatusHealthy  = "healthy"
	monitorStatusDegraded = "degraded"
	monitorStatusCritical = "critical"
	monitorStatusUnknown  = "unknown"
)

type Service struct {
	repo          Repository
	inventoryRepo serviceinventory.Repository
	metricsRepo   MetricsRepository
}

func NewService(repo Repository, inventoryRepo serviceinventory.Repository, metricsRepo MetricsRepository) *Service {
	return &Service{repo: repo, inventoryRepo: inventoryRepo, metricsRepo: metricsRepo}
}

func (s *Service) GetServicesCatalog(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceCatalogRollup, error) {
	inventoryServices, err := s.inventoryRepo.ListServices(teamID)
	if err != nil {
		return nil, err
	}
	metadata, err := s.repo.ListCatalog(teamID)
	if err != nil {
		return nil, err
	}
	metrics, err := s.metricsRepo.ListServiceMetrics(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	deployments, err := s.repo.ListTeamDeployments(teamID, 500)
	if err != nil {
		return nil, err
	}
	incidents, err := s.repo.ListTeamIncidents(teamID, 500)
	if err != nil {
		return nil, err
	}

	inventoryByService := make(map[string]serviceinventory.ServiceRecord, len(inventoryServices))
	for _, item := range inventoryServices {
		inventoryByService[item.ServiceName] = item
	}
	metadataByService := make(map[string]ServiceCatalog, len(metadata))
	for _, item := range metadata {
		metadataByService[item.ServiceName] = item
	}
	metricsByService := make(map[string]metricsRollup, len(metrics))
	for _, item := range metrics {
		metricsByService[item.ServiceName] = item
	}
	latestDeploymentByService := latestDeploymentMap(deployments)
	openIncidentCountByService, latestIncidentByService := incidentMaps(incidents)

	serviceNames := make([]string, 0, len(inventoryByService)+len(metadataByService)+len(metricsByService))
	seen := make(map[string]struct{}, len(inventoryByService)+len(metadataByService)+len(metricsByService))
	appendName := func(name string) {
		if strings.TrimSpace(name) == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		serviceNames = append(serviceNames, name)
	}
	for name := range inventoryByService {
		appendName(name)
	}
	for name := range metadataByService {
		appendName(name)
	}
	for name := range metricsByService {
		appendName(name)
	}
	slices.Sort(serviceNames)

	result := make([]ServiceCatalogRollup, 0, len(serviceNames))
	for _, serviceName := range serviceNames {
		inventoryItem, ok := inventoryByService[serviceName]
		if !ok {
			inventoryItem = fallbackInventoryRecord(teamID, serviceName)
		}
		catalog := mergeCatalog(inventoryItem, metadataByService[serviceName])
		metric, hasTraffic := metricsByService[serviceName]
		if !hasTraffic {
			metric = metricsRollup{}
		}
		monitors := buildMonitorSummary(serviceName, metric)
		scorecard := buildScorecard(catalog, monitors, openIncidentCountByService[serviceName], latestDeploymentByService[serviceName])

		latestIncident := latestIncidentByService[serviceName]
		latestDeployment := latestDeploymentByService[serviceName]
		health := monitorStatusUnknown
		if hasTraffic {
			health = healthStatus(metric)
		}
		result = append(result, ServiceCatalogRollup{
			ServiceCatalog:         catalog,
			HealthStatus:           health,
			RequestCount:           metric.RequestCount,
			ErrorCount:             metric.ErrorCount,
			ErrorRate:              safeErrorRate(metric),
			AvgLatency:             metric.AvgLatency,
			P95Latency:             metric.P95Latency,
			MonitorStatus:          monitors.Overall,
			OpenIncidentCount:      openIncidentCountByService[serviceName],
			LatestIncidentSeverity: latestIncident.Severity,
			LatestIncidentStatus:   latestIncident.Status,
			LatestDeploymentStatus: latestDeployment.Status,
			LatestDeploymentVer:    latestDeployment.Version,
			ScorecardScore:         scorecard.Score,
			HasTrafficInWindow:     hasTraffic,
		})
	}

	slices.SortFunc(result, compareCatalogRollups)
	return result, nil
}

func (s *Service) GetServiceCatalog(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (ServiceCatalog, error) {
	inventoryItem, inventoryErr := s.inventoryRepo.GetService(teamID, serviceName)
	if inventoryErr != nil && inventoryErr != sql.ErrNoRows {
		return ServiceCatalog{}, inventoryErr
	}
	override, overrideErr := s.repo.FindCatalog(teamID, serviceName)
	if overrideErr != nil && overrideErr != sql.ErrNoRows {
		return ServiceCatalog{}, overrideErr
	}

	switch {
	case inventoryErr == sql.ErrNoRows && overrideErr == sql.ErrNoRows:
		return mergeCatalog(fallbackInventoryRecord(teamID, serviceName), ServiceCatalog{}), nil
	case inventoryErr == sql.ErrNoRows:
		return override, nil
	default:
		return mergeCatalog(inventoryItem, override), nil
	}
}

func (s *Service) GetServiceOwnership(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (ServiceOwnership, error) {
	catalog, err := s.GetServiceCatalog(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return ServiceOwnership{}, err
	}
	return ServiceOwnership{
		ServiceName: catalog.ServiceName,
		OwnerTeam:   catalog.OwnerTeam,
		OwnerName:   catalog.OwnerName,
		OnCall:      catalog.OnCall,
	}, nil
}

func (s *Service) GetServiceMonitors(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (ServiceMonitorSummary, error) {
	metrics, err := s.metricsRepo.ListServiceMetrics(ctx, teamID, startMs, endMs)
	if err != nil {
		return ServiceMonitorSummary{}, err
	}
	for _, item := range metrics {
		if item.ServiceName == serviceName {
			return buildMonitorSummary(serviceName, item), nil
		}
	}
	return buildMonitorSummary(serviceName, metricsRollup{}), nil
}

func (s *Service) GetServiceDeployments(teamID int64, serviceName string) ([]ServiceDeployment, error) {
	return s.repo.ListDeployments(teamID, serviceName, 50)
}

func (s *Service) GetServiceIncidents(teamID int64, serviceName string) ([]ServiceIncident, error) {
	return s.repo.ListIncidents(teamID, serviceName, 50)
}

func (s *Service) GetServiceChangeEvents(teamID int64, serviceName string) ([]ServiceChangeEvent, error) {
	events, err := s.repo.ListChangeEvents(teamID, serviceName, 50)
	if err != nil {
		return nil, err
	}
	deployments, err := s.repo.ListDeployments(teamID, serviceName, 20)
	if err != nil {
		return nil, err
	}
	incidents, err := s.repo.ListIncidents(teamID, serviceName, 20)
	if err != nil {
		return nil, err
	}

	merged := make([]ServiceChangeEvent, 0, len(events)+len(deployments)+len(incidents))
	merged = append(merged, events...)
	for _, item := range deployments {
		merged = append(merged, ServiceChangeEvent{
			ID:               item.ID,
			ServiceName:      item.ServiceName,
			EventType:        "deployment",
			Title:            fmt.Sprintf("Deployment %s", item.Version),
			Summary:          firstNonEmpty(item.ChangeSummary, item.Summary),
			RelatedReference: item.CommitSHA,
			HappenedAt:       firstNonEmpty(item.FinishedAt, item.StartedAt),
		})
	}
	for _, item := range incidents {
		merged = append(merged, ServiceChangeEvent{
			ID:               item.ID,
			ServiceName:      item.ServiceName,
			EventType:        "incident",
			Title:            item.Title,
			Summary:          item.Summary,
			RelatedReference: item.Severity,
			HappenedAt:       item.StartedAt,
		})
	}

	slices.SortFunc(merged, func(a, b ServiceChangeEvent) int {
		return strings.Compare(b.HappenedAt, a.HappenedAt)
	})
	return merged, nil
}

func (s *Service) GetServiceScorecard(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (ServiceScorecard, error) {
	catalog, err := s.GetServiceCatalog(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return ServiceScorecard{}, err
	}
	monitors, err := s.GetServiceMonitors(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return ServiceScorecard{}, err
	}
	incidents, err := s.repo.ListIncidents(teamID, serviceName, 20)
	if err != nil {
		return ServiceScorecard{}, err
	}
	deployments, err := s.repo.ListDeployments(teamID, serviceName, 20)
	if err != nil {
		return ServiceScorecard{}, err
	}

	openIncidents := 0
	for _, item := range incidents {
		if !strings.EqualFold(item.Status, "resolved") {
			openIncidents++
		}
	}
	latestDeployment := ServiceDeployment{}
	if len(deployments) > 0 {
		latestDeployment = deployments[0]
	}
	return buildScorecard(catalog, monitors, openIncidents, latestDeployment), nil
}

func buildMonitorSummary(serviceName string, metric metricsRollup) ServiceMonitorSummary {
	items := []ServiceMonitor{
		{
			Key:       "error-budget",
			Label:     "Error Budget",
			Status:    monitorStatusUnknown,
			Severity:  monitorStatusUnknown,
			Value:     0,
			Threshold: 1.0,
			Message:   "Tracks service error rate against the healthy threshold.",
		},
		{
			Key:       "latency-p95",
			Label:     "Latency P95",
			Status:    monitorStatusUnknown,
			Severity:  monitorStatusUnknown,
			Value:     0,
			Threshold: 500,
			Message:   "Flags regressions in tail latency before they spill into user experience.",
		},
		{
			Key:       "traffic-floor",
			Label:     "Traffic Floor",
			Status:    monitorStatusUnknown,
			Severity:  monitorStatusUnknown,
			Value:     0,
			Threshold: 100,
			Message:   "Highlights sudden request collapse that usually follows upstream failures or bad deploys.",
		},
	}

	if metric.ServiceName != "" {
		errorRate := safeErrorRate(metric)
		latency := metric.P95Latency
		traffic := float64(metric.RequestCount)
		items = []ServiceMonitor{
			{
				Key:       "error-budget",
				Label:     "Error Budget",
				Status:    thresholdStatus(errorRate, 1.0, 5.0),
				Severity:  thresholdStatus(errorRate, 1.0, 5.0),
				Value:     errorRate,
				Threshold: 1.0,
				Message:   "Tracks service error rate against the healthy threshold.",
			},
			{
				Key:       "latency-p95",
				Label:     "Latency P95",
				Status:    thresholdStatus(latency, 500, 1500),
				Severity:  thresholdStatus(latency, 500, 1500),
				Value:     latency,
				Threshold: 500,
				Message:   "Flags regressions in tail latency before they spill into user experience.",
			},
			{
				Key:       "traffic-floor",
				Label:     "Traffic Floor",
				Status:    trafficStatus(traffic),
				Severity:  trafficStatus(traffic),
				Value:     traffic,
				Threshold: 100,
				Message:   "Highlights sudden request collapse that usually follows upstream failures or bad deploys.",
			},
		}
	}

	summary := ServiceMonitorSummary{ServiceName: serviceName, Overall: monitorStatusUnknown, Items: items}
	for _, item := range items {
		switch item.Status {
		case monitorStatusCritical:
			summary.Critical++
		case monitorStatusDegraded:
			summary.Degraded++
		case monitorStatusHealthy:
			summary.Healthy++
		}
		if severityRank(item.Status) > severityRank(summary.Overall) {
			summary.Overall = item.Status
		}
	}
	return summary
}

func buildScorecard(catalog ServiceCatalog, monitors ServiceMonitorSummary, openIncidents int, latestDeployment ServiceDeployment) ServiceScorecard {
	checks := []ServiceScorecardCheck{
		{Key: "owner", Label: "Owner Assigned", Passed: strings.TrimSpace(catalog.OwnerTeam) != "", Detail: firstNonEmpty(catalog.OwnerTeam, "No owner team configured")},
		{Key: "oncall", Label: "On-call Defined", Passed: strings.TrimSpace(catalog.OnCall) != "", Detail: firstNonEmpty(catalog.OnCall, "No escalation target configured")},
		{Key: "runbook", Label: "Runbook Linked", Passed: strings.TrimSpace(catalog.RunbookURL) != "", Detail: firstNonEmpty(catalog.RunbookURL, "No runbook linked")},
		{Key: "repository", Label: "Repository Linked", Passed: strings.TrimSpace(catalog.RepositoryURL) != "", Detail: firstNonEmpty(catalog.RepositoryURL, "No repository linked")},
		{Key: "dashboard", Label: "Dashboard Linked", Passed: strings.TrimSpace(catalog.DashboardURL) != "", Detail: firstNonEmpty(catalog.DashboardURL, "No dashboard linked")},
		{Key: "monitors", Label: "Monitors Healthy", Passed: monitors.Overall != monitorStatusCritical && monitors.Overall != monitorStatusUnknown, Detail: "Monitor rollup: " + monitors.Overall},
		{Key: "deployment", Label: "Recent Deployment Recorded", Passed: recentDeployment(latestDeployment), Detail: firstNonEmpty(latestDeployment.Version, "No recent deployment recorded")},
		{Key: "incidents", Label: "No Active Incident", Passed: openIncidents == 0, Detail: fmt.Sprintf("%d open incidents", openIncidents)},
	}

	passed := 0
	for _, item := range checks {
		if item.Passed {
			passed++
		}
	}

	score := 0
	if len(checks) > 0 {
		score = int(float64(passed) / float64(len(checks)) * 100)
	}

	return ServiceScorecard{
		ServiceName: catalog.ServiceName,
		Score:       score,
		Passed:      passed,
		Total:       len(checks),
		Checks:      checks,
	}
}

func mergeCatalog(inventoryItem serviceinventory.ServiceRecord, override ServiceCatalog) ServiceCatalog {
	base := ServiceCatalog{
		ServiceName:   inventoryItem.ServiceName,
		DisplayName:   inventoryItem.DisplayName,
		Description:   fmt.Sprintf("%s operational profile and ownership metadata.", firstNonEmpty(inventoryItem.DisplayName, inventoryItem.ServiceName)),
		OwnerTeam:     inventoryItem.OwnerTeam,
		OwnerName:     inventoryItem.OwnerName,
		OnCall:        inventoryItem.OnCall,
		Tier:          inventoryItem.Tier,
		Environment:   inventoryItem.Environment,
		Runtime:       inventoryItem.Runtime,
		Language:      inventoryItem.Language,
		RepositoryURL: inventoryItem.RepositoryURL,
		RunbookURL:    inventoryItem.RunbookURL,
		DashboardURL:  inventoryItem.DashboardURL,
		ServiceType:   inventoryItem.ServiceType,
		ClusterName:   inventoryItem.ClusterName,
		Tags:          inventoryItem.Tags,
		LastSeenAt:    inventoryItem.LastSeenAt,
	}
	if override.ServiceName == "" {
		return base
	}
	base.DisplayName = firstNonEmpty(override.DisplayName, base.DisplayName)
	base.Description = firstNonEmpty(override.Description, base.Description)
	base.OwnerTeam = firstNonEmpty(override.OwnerTeam, base.OwnerTeam)
	base.OwnerName = firstNonEmpty(override.OwnerName, base.OwnerName)
	base.OnCall = firstNonEmpty(override.OnCall, base.OnCall)
	base.Tier = firstNonEmpty(override.Tier, base.Tier)
	base.Environment = firstNonEmpty(override.Environment, base.Environment)
	base.Runtime = firstNonEmpty(override.Runtime, base.Runtime)
	base.Language = firstNonEmpty(override.Language, base.Language)
	base.RepositoryURL = firstNonEmpty(override.RepositoryURL, base.RepositoryURL)
	base.RunbookURL = firstNonEmpty(override.RunbookURL, base.RunbookURL)
	base.DashboardURL = firstNonEmpty(override.DashboardURL, base.DashboardURL)
	base.ServiceType = firstNonEmpty(override.ServiceType, base.ServiceType)
	base.ClusterName = firstNonEmpty(override.ClusterName, base.ClusterName)
	if len(override.Tags) > 0 {
		base.Tags = override.Tags
	}
	base.LastSeenAt = firstNonEmpty(override.LastSeenAt, base.LastSeenAt)
	return base
}

func latestDeploymentMap(deployments []ServiceDeployment) map[string]ServiceDeployment {
	latest := make(map[string]ServiceDeployment)
	for _, item := range deployments {
		if _, ok := latest[item.ServiceName]; !ok {
			latest[item.ServiceName] = item
		}
	}
	return latest
}

func incidentMaps(incidents []ServiceIncident) (map[string]int, map[string]ServiceIncident) {
	counts := make(map[string]int)
	latest := make(map[string]ServiceIncident)
	for _, item := range incidents {
		if !strings.EqualFold(item.Status, "resolved") {
			counts[item.ServiceName]++
		}
		if _, ok := latest[item.ServiceName]; !ok {
			latest[item.ServiceName] = item
		}
	}
	return counts, latest
}

func fallbackInventoryRecord(teamID int64, serviceName string) serviceinventory.ServiceRecord {
	observed := serviceinventory.BuildDefaultServiceObservation(teamID, serviceName, time.Now().UTC(), nil)
	return serviceinventory.ServiceRecord{
		TeamID:        teamID,
		ServiceName:   observed.ServiceName,
		DisplayName:   observed.DisplayName,
		OwnerTeam:     observed.OwnerTeam,
		OwnerName:     observed.OwnerName,
		OnCall:        observed.OnCall,
		Tier:          observed.Tier,
		Environment:   observed.Environment,
		Runtime:       observed.Runtime,
		Language:      observed.Language,
		RepositoryURL: observed.RepositoryURL,
		RunbookURL:    observed.RunbookURL,
		DashboardURL:  observed.DashboardURL,
		ServiceType:   observed.ServiceType,
		ClusterName:   observed.ClusterName,
		Tags:          observed.Tags,
		FirstSeenAt:   observed.FirstSeenAt.Format(time.RFC3339),
		LastSeenAt:    observed.LastSeenAt.Format(time.RFC3339),
	}
}

func compareCatalogRollups(a, b ServiceCatalogRollup) int {
	if a.OpenIncidentCount != b.OpenIncidentCount {
		return b.OpenIncidentCount - a.OpenIncidentCount
	}
	if severityRank(a.MonitorStatus) != severityRank(b.MonitorStatus) {
		return severityRank(b.MonitorStatus) - severityRank(a.MonitorStatus)
	}
	if a.HasTrafficInWindow != b.HasTrafficInWindow {
		if a.HasTrafficInWindow {
			return -1
		}
		return 1
	}
	if a.RequestCount != b.RequestCount {
		if a.RequestCount > b.RequestCount {
			return -1
		}
		return 1
	}
	return strings.Compare(a.ServiceName, b.ServiceName)
}

func thresholdStatus(value float64, degraded, critical float64) string {
	switch {
	case value >= critical:
		return monitorStatusCritical
	case value >= degraded:
		return monitorStatusDegraded
	default:
		return monitorStatusHealthy
	}
}

func trafficStatus(value float64) string {
	switch {
	case value <= 0:
		return monitorStatusUnknown
	case value < 100:
		return monitorStatusDegraded
	default:
		return monitorStatusHealthy
	}
}

func safeErrorRate(metric metricsRollup) float64 {
	if metric.RequestCount <= 0 {
		return 0
	}
	return float64(metric.ErrorCount) * 100 / float64(metric.RequestCount)
}

func healthStatus(metric metricsRollup) string {
	if metric.ServiceName == "" {
		return monitorStatusUnknown
	}
	errorRate := safeErrorRate(metric)
	switch {
	case errorRate > 5:
		return "critical"
	case errorRate > 1:
		return "degraded"
	default:
		return "healthy"
	}
}

func severityRank(status string) int {
	switch status {
	case monitorStatusCritical:
		return 3
	case monitorStatusDegraded:
		return 2
	case monitorStatusHealthy:
		return 1
	default:
		return 0
	}
}

func recentDeployment(item ServiceDeployment) bool {
	if strings.TrimSpace(item.StartedAt) == "" {
		return false
	}
	parsed, err := time.Parse(time.RFC3339, item.StartedAt)
	if err != nil {
		return false
	}
	return time.Since(parsed) <= 30*24*time.Hour
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
