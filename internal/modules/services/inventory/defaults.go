package inventory

import (
	"strings"
	"time"
)

func BuildDefaultServiceObservation(teamID int64, serviceName string, observedAt time.Time, attrs map[string]string) ServiceObservation {
	environment := firstNonEmpty(
		attrs["deployment.environment"],
		attrs["deployment.environment.name"],
		"production",
	)
	runtime := firstNonEmpty(attrs["process.runtime.name"], inferRuntime(serviceName))
	language := firstNonEmpty(attrs["telemetry.sdk.language"], inferLanguage(runtime, serviceName))
	clusterName := firstNonEmpty(
		attrs["k8s.cluster.name"],
		attrs["k8s.namespace.name"],
		attrs["service.namespace"],
		inferClusterName(serviceName),
	)
	serviceType := inferServiceType(serviceName, attrs)
	displayName := humanizeServiceName(serviceName)
	ownerTeam := defaultOwnerTeam(clusterName)
	tier := defaultTier(serviceType)

	tags := []string{serviceType}
	if environment != "" {
		tags = append(tags, environment)
	}
	if clusterName != "" {
		tags = append(tags, clusterName)
	}

	return ServiceObservation{
		TeamID:        teamID,
		ServiceName:   serviceName,
		DisplayName:   displayName,
		OwnerTeam:     ownerTeam,
		Tier:          tier,
		Environment:   environment,
		Runtime:       runtime,
		Language:      language,
		DashboardURL:  "/services/" + serviceName,
		ServiceType:   serviceType,
		ClusterName:   clusterName,
		Tags:          tags,
		FirstSeenAt:   observedAt.UTC(),
		LastSeenAt:    observedAt.UTC(),
	}
}

func defaultOwnerTeam(clusterName string) string {
	if strings.TrimSpace(clusterName) == "" {
		return "Platform"
	}
	return humanizeServiceName(clusterName) + " Team"
}

func defaultTier(serviceType string) string {
	switch serviceType {
	case "database", "queue", "external":
		return "tier-1"
	default:
		return "tier-2"
	}
}

func inferClusterName(serviceName string) string {
	parts := splitServiceName(serviceName)
	if len(parts) >= 2 {
		return parts[0]
	}
	return ""
}

func inferServiceType(name string, attrs map[string]string) string {
	if strings.TrimSpace(attrs["db.system"]) != "" {
		return "database"
	}
	if strings.TrimSpace(attrs["messaging.system"]) != "" {
		return "queue"
	}
	if strings.EqualFold(attrs["rpc.system"], "grpc") || strings.Contains(strings.ToLower(name), "grpc") {
		return "grpc"
	}

	lower := strings.ToLower(name)
	switch {
	case containsAny(lower, "postgres", "mysql", "maria", "mongo", "clickhouse", "sqlite", "cockroach", "database"):
		return "database"
	case containsAny(lower, "redis", "memcache", "elasticache", "valkey", "cache"):
		return "cache"
	case containsAny(lower, "kafka", "rabbit", "nats", "sqs", "pulsar", "amqp", "queue"):
		return "queue"
	case containsAny(lower, ".com", ".io", ".net", ".org", "external", "api."):
		return "external"
	default:
		return "application"
	}
}

func inferRuntime(serviceName string) string {
	lower := strings.ToLower(serviceName)
	switch {
	case containsAny(lower, "catalog", "checkout", "gateway", "edge"):
		return "nodejs"
	case containsAny(lower, "search", "analytics", "recommendation"):
		return "jvm"
	case containsAny(lower, "ml", "model", "inference"):
		return "python"
	default:
		return "go"
	}
}

func inferLanguage(runtime, serviceName string) string {
	if runtime != "" {
		switch strings.ToLower(runtime) {
		case "node", "nodejs":
			return "typescript"
		case "python":
			return "python"
		case "jvm", "java":
			return "java"
		case "go":
			return "go"
		}
	}

	lower := strings.ToLower(serviceName)
	switch {
	case containsAny(lower, "catalog", "checkout", "gateway", "edge"):
		return "typescript"
	case containsAny(lower, "search", "analytics", "recommendation"):
		return "java"
	case containsAny(lower, "ml", "model", "inference"):
		return "python"
	default:
		return "go"
	}
}

func splitServiceName(name string) []string {
	return strings.FieldsFunc(name, func(r rune) bool {
		return r == '-' || r == '_' || r == '.'
	})
}

func humanizeServiceName(value string) string {
	if value == "" {
		return ""
	}
	parts := splitServiceName(value)
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func containsAny(value string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(value, sub) {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
