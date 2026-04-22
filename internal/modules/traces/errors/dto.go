package errors

type ErrorGroupsRequest struct {
	StartMs     int64    `form:"startTime" binding:"required"`
	EndMs       int64    `form:"endTime" binding:"required"`
	Services    []string `form:"services"`
	Limit       int      `form:"limit"`
	ServiceName string   `form:"-"`
}

type ErrorGroupsResponse struct {
	Groups []ErrorGroup `json:"groups"`
}

type ErrorGroup struct {
	ExceptionType string `json:"exception_type"`
	StatusMessage string `json:"status_message"`
	Service       string `json:"service"`
	Count         uint64 `json:"count"`
}

type TimeseriesRequest struct {
	StartMs     int64  `form:"startTime" binding:"required"`
	EndMs       int64  `form:"endTime" binding:"required"`
	ServiceName string `form:"-"`
}

type TimeseriesResponse struct {
	Buckets []TimeseriesBucket `json:"buckets"`
}

type TimeseriesBucket struct {
	TimeBucket string `json:"time_bucket"`
	Errors     uint64 `json:"errors"`
	Total      uint64 `json:"total"`
}
