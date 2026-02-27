package model

import "time"

// SavedLogSearch represents a saved log search query.
type SavedLogSearch struct {
	ID        string    `json:"id"`
	TeamUUID  string    `json:"teamUuid"`
	Name      string    `json:"name"`
	Filters   string    `json:"filters"`   // JSON-encoded filter array
	Search    string    `json:"search"`    // Free-text search
	IsDefault bool      `json:"isDefault"` // Pinned to top
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// LogPattern represents a group of similar log messages.
type LogPattern struct {
	Pattern    string `json:"pattern"`
	Sample     string `json:"sample"`
	Count      int64  `json:"count"`
	Level      string `json:"level"`
	Service    string `json:"service"`
	Percentage float64 `json:"percentage"`
}

// LogPatternsResponse is the response for the patterns endpoint.
type LogPatternsResponse struct {
	Patterns []LogPattern `json:"patterns"`
	Total    int64        `json:"total"`
}
