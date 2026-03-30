package cpu

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

type LoadAverageResult struct {
	Load1m  float64 `ch:"load_1m"  json:"load_1m"`
	Load5m  float64 `ch:"load_5m"  json:"load_5m"`
	Load15m float64 `ch:"load_15m" json:"load_15m"`
}
