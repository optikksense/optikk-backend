package network

type DirectionBucket struct {
	Timestamp string   `json:"timestamp"`
	Direction string   `json:"direction"`
	Value     *float64 `json:"value"`
}

type StateBucket struct {
	Timestamp string   `json:"timestamp"`
	State     string   `json:"state"`
	Value     *float64 `json:"value"`
}

type ResourceBucket struct {
	Timestamp string   `json:"timestamp"`
	Pod       string   `json:"pod"`
	Value     *float64 `json:"value"`
}
