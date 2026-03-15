package dashboards

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

func (r *Repository) CreateWidget(dashboardID, teamID int64, req CreateWidgetRequest) (*Widget, error) {
	result, err := r.db.Exec(`
		INSERT INTO dashboard_widgets (dashboard_id, team_id, title, chart_type, query_json, position_json)
		VALUES (?, ?, ?, ?, ?, ?)
	`, dashboardID, teamID, req.Title, req.ChartType, req.QueryJSON, req.PositionJSON)
	if err != nil {
		return nil, err
	}
	id, _ := result.LastInsertId()
	return r.GetWidgetByID(teamID, id)
}

func (r *Repository) GetWidgetByID(teamID, widgetID int64) (*Widget, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT id, dashboard_id, team_id, title, chart_type, query_json, position_json, created_at, updated_at
		FROM dashboard_widgets WHERE id = ? AND team_id = ?
	`, widgetID, teamID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return widgetFromRow(row), nil
}

func (r *Repository) ListWidgets(dashboardID, teamID int64) ([]Widget, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT id, dashboard_id, team_id, title, chart_type, query_json, position_json, created_at, updated_at
		FROM dashboard_widgets
		WHERE dashboard_id = ? AND team_id = ?
		ORDER BY id ASC
	`, dashboardID, teamID)
	if err != nil {
		return nil, err
	}
	widgets := make([]Widget, 0, len(rows))
	for _, row := range rows {
		widgets = append(widgets, *widgetFromRow(row))
	}
	return widgets, nil
}

func (r *Repository) UpdateWidget(teamID, widgetID int64, req UpdateWidgetRequest) (*Widget, error) {
	setParts := []string{}
	args := []any{}
	if req.Title != nil {
		setParts = append(setParts, "title = ?")
		args = append(args, *req.Title)
	}
	if req.ChartType != nil {
		setParts = append(setParts, "chart_type = ?")
		args = append(args, *req.ChartType)
	}
	if req.QueryJSON != nil {
		setParts = append(setParts, "query_json = ?")
		args = append(args, *req.QueryJSON)
	}
	if req.PositionJSON != nil {
		setParts = append(setParts, "position_json = ?")
		args = append(args, *req.PositionJSON)
	}
	if len(setParts) == 0 {
		return r.GetWidgetByID(teamID, widgetID)
	}

	query := "UPDATE dashboard_widgets SET "
	for i, p := range setParts {
		if i > 0 {
			query += ", "
		}
		query += p
	}
	query += " WHERE id = ? AND team_id = ?"
	args = append(args, widgetID, teamID)

	_, err := r.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return r.GetWidgetByID(teamID, widgetID)
}

func (r *Repository) DeleteWidget(teamID, widgetID int64) error {
	result, err := r.db.Exec(`DELETE FROM dashboard_widgets WHERE id = ? AND team_id = ?`, widgetID, teamID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("widget not found")
	}
	return nil
}

func (r *Repository) DeleteWidgetsByDashboard(dashboardID, teamID int64) error {
	_, err := r.db.Exec(`DELETE FROM dashboard_widgets WHERE dashboard_id = ? AND team_id = ?`, dashboardID, teamID)
	return err
}

func widgetFromRow(row map[string]any) *Widget {
	return &Widget{
		ID:           dbutil.Int64FromAny(row["id"]),
		DashboardID:  dbutil.Int64FromAny(row["dashboard_id"]),
		TeamID:       dbutil.Int64FromAny(row["team_id"]),
		Title:        dbutil.StringFromAny(row["title"]),
		ChartType:    dbutil.StringFromAny(row["chart_type"]),
		QueryJSON:    dbutil.StringFromAny(row["query_json"]),
		PositionJSON: dbutil.StringFromAny(row["position_json"]),
		CreatedAt:    dbutil.TimeFromAny(row["created_at"]),
		UpdatedAt:    dbutil.TimeFromAny(row["updated_at"]),
	}
}
