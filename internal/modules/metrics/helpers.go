package metrics

func calcErrorRate(total, errors int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(errors) * 100.0 / float64(total)
}

func topologyNodeStatus(errorRate float64) string {
	switch {
	case errorRate > 5:
		return "unhealthy"
	case errorRate > 1:
		return "degraded"
	default:
		return "healthy"
	}
}

func dashboardServiceStatus(errorRate float64) string {
	switch {
	case errorRate > 10:
		return "DEGRADED"
	case errorRate > 0:
		return "WARNING"
	default:
		return "HEALTHY"
	}
}
