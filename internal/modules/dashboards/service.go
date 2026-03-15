package dashboards

import "fmt"

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) CreateDashboard(teamID, userID int64, req CreateDashboardRequest) (*Dashboard, error) {
	return s.repo.Create(teamID, userID, req)
}

func (s *Service) GetDashboard(teamID, id int64) (*DashboardWithWidgets, error) {
	dash, err := s.repo.GetByID(teamID, id)
	if err != nil {
		return nil, err
	}
	if dash == nil {
		return nil, nil
	}
	widgets, err := s.repo.ListWidgets(id, teamID)
	if err != nil {
		return nil, err
	}
	return &DashboardWithWidgets{Dashboard: *dash, Widgets: widgets}, nil
}

func (s *Service) ListDashboards(teamID, userID int64) ([]Dashboard, error) {
	return s.repo.List(teamID, userID)
}

func (s *Service) UpdateDashboard(teamID, userID, id int64, req UpdateDashboardRequest) (*Dashboard, error) {
	existing, err := s.repo.GetByID(teamID, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("dashboard not found")
	}
	if existing.CreatedBy != userID {
		return nil, fmt.Errorf("only the creator can update this dashboard")
	}
	return s.repo.Update(teamID, id, req)
}

func (s *Service) DeleteDashboard(teamID, userID, id int64) error {
	existing, err := s.repo.GetByID(teamID, id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("dashboard not found")
	}
	if existing.CreatedBy != userID {
		return fmt.Errorf("only the creator can delete this dashboard")
	}
	if err := s.repo.DeleteWidgetsByDashboard(id, teamID); err != nil {
		return err
	}
	return s.repo.Delete(teamID, id)
}

func (s *Service) AddWidget(teamID, dashboardID int64, req CreateWidgetRequest) (*Widget, error) {
	dash, err := s.repo.GetByID(teamID, dashboardID)
	if err != nil {
		return nil, err
	}
	if dash == nil {
		return nil, fmt.Errorf("dashboard not found")
	}
	return s.repo.CreateWidget(dashboardID, teamID, req)
}

func (s *Service) UpdateWidget(teamID, widgetID int64, req UpdateWidgetRequest) (*Widget, error) {
	existing, err := s.repo.GetWidgetByID(teamID, widgetID)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("widget not found")
	}
	return s.repo.UpdateWidget(teamID, widgetID, req)
}

func (s *Service) DeleteWidget(teamID, widgetID int64) error {
	return s.repo.DeleteWidget(teamID, widgetID)
}

func (s *Service) DuplicateDashboard(teamID, userID, dashboardID int64) (*DashboardWithWidgets, error) {
	source, err := s.GetDashboard(teamID, dashboardID)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, fmt.Errorf("dashboard not found")
	}

	newDash, err := s.repo.Create(teamID, userID, CreateDashboardRequest{
		Name:        source.Name + " (copy)",
		Description: source.Description,
		IsShared:    false,
	})
	if err != nil {
		return nil, err
	}

	// Update layout to match source
	if source.LayoutJSON != "" && source.LayoutJSON != "{}" {
		layoutCopy := source.LayoutJSON
		newDash, err = s.repo.Update(teamID, newDash.ID, UpdateDashboardRequest{LayoutJSON: &layoutCopy})
		if err != nil {
			return nil, err
		}
	}

	newWidgets := make([]Widget, 0, len(source.Widgets))
	for _, w := range source.Widgets {
		nw, err := s.repo.CreateWidget(newDash.ID, teamID, CreateWidgetRequest{
			Title:        w.Title,
			ChartType:    w.ChartType,
			QueryJSON:    w.QueryJSON,
			PositionJSON: w.PositionJSON,
		})
		if err != nil {
			return nil, err
		}
		newWidgets = append(newWidgets, *nw)
	}

	return &DashboardWithWidgets{Dashboard: *newDash, Widgets: newWidgets}, nil
}
