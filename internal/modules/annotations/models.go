package annotations

import "time"

type Annotation struct {
	ID          int64     `json:"id"`
	TeamID      int64     `json:"teamId"`
	CreatedBy   int64     `json:"createdBy"`
	Title       string    `json:"title"`
	Description string    `json:"description,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	EndTime     *time.Time `json:"endTime,omitempty"`
	Tags        string    `json:"tags,omitempty"`
	Source      string    `json:"source"`
	ServiceName string    `json:"serviceName,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type CreateRequest struct {
	Title       string  `json:"title" binding:"required"`
	Description string  `json:"description"`
	Timestamp   int64   `json:"timestamp" binding:"required"`
	EndTime     *int64  `json:"endTime"`
	Tags        string  `json:"tags"`
	Source      string  `json:"source"`
	ServiceName string  `json:"serviceName"`
}

type UpdateRequest struct {
	Title       *string `json:"title"`
	Description *string `json:"description"`
	Tags        *string `json:"tags"`
	ServiceName *string `json:"serviceName"`
}
