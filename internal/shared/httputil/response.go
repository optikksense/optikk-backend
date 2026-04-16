package httputil

import "time"

type APIResponse struct {
	Success    bool         `json:"success"`
	Data       any          `json:"data,omitempty"`
	Error      *ErrorDetail `json:"error,omitempty"`
	Pagination *PageInfo    `json:"pagination,omitempty"`
	Timestamp  time.Time    `json:"timestamp"`
}

type ErrorDetail struct {
	Code        string            `json:"code"`
	Message     string            `json:"message"`
	Timestamp   time.Time         `json:"timestamp"`
	Path        string            `json:"path,omitempty"`
	RequestID   string            `json:"requestId,omitempty"`
	FieldErrors map[string]string `json:"fieldErrors,omitempty"`
}

type PageInfo struct {
	Page          int   `json:"page"`
	Size          int   `json:"size"`
	TotalElements int64 `json:"totalElements"`
	TotalPages    int   `json:"totalPages"`
	HasNext       bool  `json:"hasNext"`
	HasPrevious   bool  `json:"hasPrevious"`
}

func Success(data any) APIResponse {
	return APIResponse{Success: true, Data: data, Timestamp: time.Now().UTC()}
}

func Failure(code, msg, path string, requestID ...string) APIResponse {
	detail := &ErrorDetail{
		Code:      code,
		Message:   msg,
		Timestamp: time.Now().UTC(),
		Path:      path,
	}
	if len(requestID) > 0 {
		detail.RequestID = requestID[0]
	}
	return APIResponse{
		Success:   false,
		Error:     detail,
		Timestamp: time.Now().UTC(),
	}
}
