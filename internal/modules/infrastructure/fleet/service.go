package fleet

type Service interface {
	GetFleetPods(teamID int64, startMs, endMs int64) ([]FleetPod, error)
}

type fleetService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &fleetService{repo: repo}
}

func (s *fleetService) GetFleetPods(teamID int64, startMs, endMs int64) ([]FleetPod, error) {
	return s.repo.GetFleetPods(teamID, startMs, endMs)
}
