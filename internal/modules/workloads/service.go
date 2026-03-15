package workloads

import "fmt"

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Create(teamID, userID int64, req CreateWorkloadRequest) (*Workload, error) {
	return s.repo.Create(teamID, userID, req)
}

func (s *Service) Get(teamID, id int64) (*Workload, error) {
	return s.repo.GetByID(teamID, id)
}

func (s *Service) List(teamID int64) ([]Workload, error) {
	return s.repo.List(teamID)
}

func (s *Service) Update(teamID, userID, id int64, req UpdateWorkloadRequest) (*Workload, error) {
	existing, err := s.repo.GetByID(teamID, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("workload not found")
	}
	if existing.CreatedBy != userID {
		return nil, fmt.Errorf("only the creator can update this workload")
	}
	return s.repo.Update(teamID, id, req)
}

func (s *Service) Delete(teamID, userID, id int64) error {
	existing, err := s.repo.GetByID(teamID, id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("workload not found")
	}
	if existing.CreatedBy != userID {
		return fmt.Errorf("only the creator can delete this workload")
	}
	return s.repo.Delete(teamID, id)
}
