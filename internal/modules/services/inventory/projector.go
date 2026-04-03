package inventory

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

type Projector struct {
	repo          Repository
	bootstrapRepo BootstrapRepository
}

func NewProjector(repo Repository, bootstrapRepo BootstrapRepository) *Projector {
	return &Projector{repo: repo, bootstrapRepo: bootstrapRepo}
}

func (p *Projector) BootstrapFromHistory(ctx context.Context) error {
	if p == nil || p.bootstrapRepo == nil || p.repo == nil {
		return nil
	}
	services, err := p.bootstrapRepo.ListObservedServices(ctx)
	if err != nil {
		return err
	}
	if err := p.repo.UpsertServices(services); err != nil {
		return err
	}
	dependencies, err := p.bootstrapRepo.ListObservedDependencies(ctx)
	if err != nil {
		return err
	}
	if err := p.repo.UpsertDependencies(dependencies); err != nil {
		return err
	}
	logger.L().Info("services inventory bootstrap complete",
		slog.Int("services", len(services)),
		slog.Int("dependencies", len(dependencies)),
	)
	return nil
}

func (p *Projector) ObserveTraceExport(teamID int64, req *tracepb.ExportTraceServiceRequest) error {
	if p == nil || p.repo == nil || req == nil {
		return nil
	}

	services, dependencies := collectObservations(teamID, req)
	if err := p.repo.UpsertServices(services); err != nil {
		return err
	}
	if err := p.repo.UpsertDependencies(dependencies); err != nil {
		return err
	}
	return nil
}

type spanRecord struct {
	serviceName string
	startedAt   time.Time
	parentSpan  string
}

func collectObservations(teamID int64, req *tracepb.ExportTraceServiceRequest) ([]ServiceObservation, []DependencyObservation) {
	serviceByKey := make(map[string]ServiceObservation)
	spanByID := make(map[string]spanRecord)
	dependencyByKey := make(map[string]DependencyObservation)

	for _, rs := range req.ResourceSpans {
		resourceAttrs := map[string]string{}
		if rs.Resource != nil {
			resourceAttrs = attrsToMap(rs.Resource.Attributes)
		}
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				spanAttrs := attrsToMap(span.Attributes)
				merged := mergeAttrs(resourceAttrs, spanAttrs)
				serviceName := firstNonEmpty(merged["service.name"], resourceAttrs["service.name"])
				if strings.TrimSpace(serviceName) == "" {
					continue
				}

				observedAt := nanoToTime(span.StartTimeUnixNano)
				if observedAt.IsZero() {
					observedAt = time.Now().UTC()
				}
				key := serviceObservationKey(teamID, serviceName)
				existing, ok := serviceByKey[key]
				current := BuildDefaultServiceObservation(teamID, serviceName, observedAt, merged)
				if ok {
					current = mergeServiceObservation(existing, current)
				}
				serviceByKey[key] = current

				spanID := bytesToHex(span.SpanId)
				if spanID != "" {
					spanByID[spanID] = spanRecord{
						serviceName: serviceName,
						startedAt:   observedAt,
						parentSpan:  bytesToHex(span.ParentSpanId),
					}
				}
			}
		}
	}

	for _, span := range spanByID {
		if strings.TrimSpace(span.parentSpan) == "" {
			continue
		}
		parent, ok := spanByID[span.parentSpan]
		if !ok || parent.serviceName == span.serviceName || parent.serviceName == "" || span.serviceName == "" {
			continue
		}
		key := dependencyObservationKey(teamID, parent.serviceName, span.serviceName, "service")
		current := DependencyObservation{
			TeamID:         teamID,
			SourceService:  parent.serviceName,
			TargetService:  span.serviceName,
			DependencyKind: "service",
			FirstSeenAt:    minTime(parent.startedAt, span.startedAt),
			LastSeenAt:     maxTime(parent.startedAt, span.startedAt),
		}
		if existing, ok := dependencyByKey[key]; ok {
			current = mergeDependencyObservation(existing, current)
		}
		dependencyByKey[key] = current
	}

	services := make([]ServiceObservation, 0, len(serviceByKey))
	for _, item := range serviceByKey {
		services = append(services, item)
	}
	dependencies := make([]DependencyObservation, 0, len(dependencyByKey))
	for _, item := range dependencyByKey {
		dependencies = append(dependencies, item)
	}

	return services, dependencies
}

func attrsToMap(attrs []*commonpb.KeyValue) map[string]string {
	if len(attrs) == 0 {
		return map[string]string{}
	}
	result := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		result[attr.Key] = anyValueString(attr.Value)
	}
	return result
}

func mergeAttrs(resourceAttrs, spanAttrs map[string]string) map[string]string {
	merged := make(map[string]string, len(resourceAttrs)+len(spanAttrs))
	for key, value := range resourceAttrs {
		merged[key] = value
	}
	for key, value := range spanAttrs {
		merged[key] = value
	}
	return merged
}

func mergeServiceObservation(existing, current ServiceObservation) ServiceObservation {
	current.FirstSeenAt = minTime(existing.FirstSeenAt, current.FirstSeenAt)
	current.LastSeenAt = maxTime(existing.LastSeenAt, current.LastSeenAt)
	current.DisplayName = firstNonEmpty(current.DisplayName, existing.DisplayName)
	current.OwnerTeam = firstNonEmpty(current.OwnerTeam, existing.OwnerTeam)
	current.OwnerName = firstNonEmpty(current.OwnerName, existing.OwnerName)
	current.OnCall = firstNonEmpty(current.OnCall, existing.OnCall)
	current.Tier = firstNonEmpty(current.Tier, existing.Tier)
	current.Environment = firstNonEmpty(current.Environment, existing.Environment)
	current.Runtime = firstNonEmpty(current.Runtime, existing.Runtime)
	current.Language = firstNonEmpty(current.Language, existing.Language)
	current.RepositoryURL = firstNonEmpty(current.RepositoryURL, existing.RepositoryURL)
	current.RunbookURL = firstNonEmpty(current.RunbookURL, existing.RunbookURL)
	current.DashboardURL = firstNonEmpty(current.DashboardURL, existing.DashboardURL)
	current.ServiceType = firstNonEmpty(current.ServiceType, existing.ServiceType)
	current.ClusterName = firstNonEmpty(current.ClusterName, existing.ClusterName)
	if len(existing.Tags) > len(current.Tags) {
		current.Tags = existing.Tags
	}
	return current
}

func mergeDependencyObservation(existing, current DependencyObservation) DependencyObservation {
	current.FirstSeenAt = minTime(existing.FirstSeenAt, current.FirstSeenAt)
	current.LastSeenAt = maxTime(existing.LastSeenAt, current.LastSeenAt)
	return current
}

func serviceObservationKey(teamID int64, serviceName string) string {
	return fmt.Sprintf("%d::%s", teamID, strings.TrimSpace(serviceName))
}

func dependencyObservationKey(teamID int64, source, target, kind string) string {
	return strings.Join([]string{
		fmt.Sprintf("%d", teamID),
		strings.TrimSpace(kind),
		strings.TrimSpace(source),
		strings.TrimSpace(target),
	}, "::")
}

func minTime(values ...time.Time) time.Time {
	var result time.Time
	for _, value := range values {
		if value.IsZero() {
			continue
		}
		if result.IsZero() || value.Before(result) {
			result = value
		}
	}
	if result.IsZero() {
		return time.Now().UTC()
	}
	return result.UTC()
}

func maxTime(values ...time.Time) time.Time {
	var result time.Time
	for _, value := range values {
		if value.After(result) {
			result = value
		}
	}
	if result.IsZero() {
		return time.Now().UTC()
	}
	return result.UTC()
}

func anyValueString(value *commonpb.AnyValue) string {
	if value == nil {
		return ""
	}
	switch item := value.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return item.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(item.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(item.DoubleValue, 'f', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		if item.BoolValue {
			return "true"
		}
		return "false"
	case *commonpb.AnyValue_BytesValue:
		return string(item.BytesValue)
	default:
		return ""
	}
}

func nanoToTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Now().UTC()
	}
	return time.Unix(0, int64(ns)).UTC() //nolint:gosec // G115
}

func bytesToHex(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	return hex.EncodeToString(value)
}
