package annotations

import "fmt"

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Create(teamID, userID int64, req CreateRequest) (*Annotation, error) {
	if req.Title == "" {
		return nil, fmt.Errorf("title is required")
	}
	if req.Timestamp == 0 {
		return nil, fmt.Errorf("timestamp is required")
	}
	return s.repo.Create(teamID, userID, req)
}

func (s *Service) List(teamID int64, startMs, endMs int64, serviceName string) ([]Annotation, error) {
	return s.repo.List(teamID, startMs, endMs, serviceName)
}

func (s *Service) GetByID(teamID, id int64) (*Annotation, error) {
	return s.repo.GetByID(teamID, id)
}

func (s *Service) Update(teamID, userID, id int64, req UpdateRequest) (*Annotation, error) {
	existing, err := s.repo.GetByID(teamID, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("annotation not found")
	}
	if existing.CreatedBy != userID {
		return nil, fmt.Errorf("only the creator can update this annotation")
	}
	return s.repo.Update(teamID, id, req)
}

func (s *Service) Delete(teamID, userID, id int64) error {
	existing, err := s.repo.GetByID(teamID, id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("annotation not found")
	}
	if existing.CreatedBy != userID {
		return fmt.Errorf("only the creator can delete this annotation")
	}
	return s.repo.Delete(teamID, id)
}
