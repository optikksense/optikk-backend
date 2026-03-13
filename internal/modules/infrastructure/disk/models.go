package disk

type DirectionBucket struct {
	Timestamp string   `json:"timestamp"`
	Direction string   `json:"direction"`
	Value     *float64 `json:"value"`
}

type ResourceBucket struct {
	Timestamp string   `json:"timestamp"`
	Pod       string   `json:"pod"`
	Value     *float64 `json:"value"`
}

type MountpointBucket struct {
	Timestamp  string   `json:"timestamp"`
	Mountpoint string   `json:"mountpoint"`
	Value      *float64 `json:"value"`
}
