package disk

type DirectionBucket struct {
	Timestamp string   `ch:"time_bucket" json:"timestamp"`
	Direction string   `ch:"direction"   json:"direction"`
	Value     *float64 `ch:"metric_val"  json:"value"`
}

type ResourceBucket struct {
	Timestamp string   `ch:"time_bucket" json:"timestamp"`
	Pod       string   `ch:"pod"         json:"pod"`
	Value     *float64 `ch:"metric_val"  json:"value"`
}

type MountpointBucket struct {
	Timestamp  string   `ch:"time_bucket" json:"timestamp"`
	Mountpoint string   `ch:"mountpoint"  json:"mountpoint"`
	Value      *float64 `ch:"metric_val"  json:"value"`
}
