package savedviews

import "fmt"

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Create(teamID, userID int64, req CreateViewRequest) (*SavedView, error) {
	if err := validateViewType(req.ViewType); err != nil {
		return nil, err
	}
	return s.repo.Create(teamID, userID, req)
}

func (s *Service) List(teamID, userID int64) ([]SavedView, error) {
	return s.repo.List(teamID, userID)
}

func (s *Service) GetByID(teamID, id int64) (*SavedView, error) {
	return s.repo.GetByID(teamID, id)
}

func (s *Service) Update(teamID, userID, id int64, req UpdateViewRequest) (*SavedView, error) {
	existing, err := s.repo.GetByID(teamID, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("saved view not found")
	}
	if existing.CreatedBy != userID {
		return nil, fmt.Errorf("only the creator can update this view")
	}
	return s.repo.Update(teamID, id, req)
}

func (s *Service) Delete(teamID, userID, id int64) error {
	existing, err := s.repo.GetByID(teamID, id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("saved view not found")
	}
	if existing.CreatedBy != userID {
		return fmt.Errorf("only the creator can delete this view")
	}
	return s.repo.Delete(teamID, id)
}

func validateViewType(vt string) error {
	switch vt {
	case "trace_search", "analytics":
		return nil
	default:
		return fmt.Errorf("invalid view_type: %q (must be trace_search or analytics)", vt)
	}
}
