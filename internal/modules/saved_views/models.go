package savedviews

import "time"

type SavedView struct {
	ID         int64     `json:"id"`
	TeamID     int64     `json:"team_id"`
	UserID     int64     `json:"user_id"`
	Scope      string    `json:"scope"`
	Name       string    `json:"name"`
	URL        string    `json:"url"`
	Visibility string    `json:"visibility"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type CreateRequest struct {
	Scope      string `json:"scope" binding:"required"`
	Name       string `json:"name" binding:"required"`
	URL        string `json:"url" binding:"required"`
	Visibility string `json:"visibility"`
}

type UpdateRequest struct {
	Name       string `json:"name"`
	URL        string `json:"url"`
	Visibility string `json:"visibility"`
}
