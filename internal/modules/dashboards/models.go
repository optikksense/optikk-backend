package dashboards

import "time"

type Dashboard struct {
	ID          int64     `json:"id"`
	TeamID      int64     `json:"teamId"`
	CreatedBy   int64     `json:"createdBy"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	IsShared    bool      `json:"isShared"`
	LayoutJSON  string    `json:"layoutJson"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Widget struct {
	ID           int64     `json:"id"`
	DashboardID  int64     `json:"dashboardId"`
	TeamID       int64     `json:"teamId"`
	Title        string    `json:"title"`
	ChartType    string    `json:"chartType"`
	QueryJSON    string    `json:"queryJson"`
	PositionJSON string    `json:"positionJson"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type DashboardWithWidgets struct {
	Dashboard
	Widgets []Widget `json:"widgets"`
}

type CreateDashboardRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	IsShared    bool   `json:"isShared"`
}

type UpdateDashboardRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	IsShared    *bool   `json:"isShared"`
	LayoutJSON  *string `json:"layoutJson"`
}

type CreateWidgetRequest struct {
	Title        string `json:"title" binding:"required"`
	ChartType    string `json:"chartType" binding:"required"`
	QueryJSON    string `json:"queryJson" binding:"required"`
	PositionJSON string `json:"positionJson" binding:"required"`
}

type UpdateWidgetRequest struct {
	Title        *string `json:"title"`
	ChartType    *string `json:"chartType"`
	QueryJSON    *string `json:"queryJson"`
	PositionJSON *string `json:"positionJson"`
}
