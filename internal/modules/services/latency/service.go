package latency

import "context"

type Service struct{ repo *Repository }

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

func (s *Service) Histogram(ctx context.Context, teamID int64, req HistogramRequest) (HistogramResponse, error) {
	row, err := s.repo.Histogram(ctx, teamID, req)
	if err != nil {
		return HistogramResponse{}, err
	}
	return HistogramResponse{
		P50: float64(row.P50),
		P90: float64(row.P90),
		P95: float64(row.P95),
		P99: float64(row.P99),
		Max: row.Max,
		Avg: row.Avg,
	}, nil
}

func (s *Service) Heatmap(ctx context.Context, teamID int64, req HeatmapRequest) (HeatmapResponse, error) {
	rows, err := s.repo.Heatmap(ctx, teamID, req)
	if err != nil {
		return HeatmapResponse{}, err
	}
	cells := make([]HeatmapCell, len(rows))
	for i, r := range rows {
		cells[i] = HeatmapCell{TimeBucket: r.TimeBucket, BucketMs: r.BucketMs, Count: r.Count}
	}
	return HeatmapResponse{Cells: cells}, nil
}
