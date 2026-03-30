package network

type DirectionBucket struct {
	Timestamp string   `ch:"time_bucket" json:"timestamp"`
	Direction string   `ch:"direction"   json:"direction"`
	Value     *float64 `ch:"metric_val"  json:"value"`
}

type StateBucket struct {
	Timestamp string   `ch:"time_bucket" json:"timestamp"`
	State     string   `ch:"state"        json:"state"`
	Value     *float64 `ch:"metric_val"   json:"value"`
}

type ResourceBucket struct {
	Timestamp string   `ch:"time_bucket" json:"timestamp"`
	Pod       string   `ch:"pod"         json:"pod"`
	Value     *float64 `ch:"metric_val"  json:"value"`
}
