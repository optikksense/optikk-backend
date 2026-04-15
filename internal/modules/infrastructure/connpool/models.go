package connpool

type MetricValue struct {
	Value float64 `json:"value"`
}

type ConnPoolServiceMetric struct {
	ServiceName string   `json:"service_name"`
	Value       *float64 `json:"value"`
}

type ConnPoolInstanceMetric struct {
	Host        string   `json:"host"`
	Pod         string   `json:"pod"`
	Container   string   `json:"container"`
	ServiceName string   `json:"service_name"`
	Value       *float64 `json:"value"`
}
