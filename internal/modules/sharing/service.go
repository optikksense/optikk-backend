package sharing

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"time"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) CreateLink(teamID, userID int64, req CreateShareRequest) (*SharedLink, error) {
	token, err := GenerateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate token: %w", err)
	}

	var expiresAt any
	if req.ExpiresInHours != nil && *req.ExpiresInHours > 0 {
		t := time.Now().Add(time.Duration(*req.ExpiresInHours) * time.Hour)
		expiresAt = t
	}

	return s.repo.Create(teamID, userID, token, req.ResourceType, req.ResourceID, expiresAt)
}

func (s *Service) ResolveLink(token string) (*SharedLink, error) {
	link, err := s.repo.GetByToken(token)
	if err != nil {
		return nil, err
	}
	if link == nil {
		return nil, fmt.Errorf("shared link not found")
	}
	if link.ExpiresAt != nil && link.ExpiresAt.Before(time.Now()) {
		return nil, fmt.Errorf("shared link has expired")
	}
	return link, nil
}

func (s *Service) ListLinks(teamID int64) ([]SharedLink, error) {
	return s.repo.ListByTeam(teamID)
}

func (s *Service) DeleteLink(teamID, userID, id int64) error {
	return s.repo.Delete(teamID, id)
}

func (s *Service) ExportCSV(columns []string, data []map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	if err := w.Write(columns); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, row := range data {
		record := make([]string, len(columns))
		for i, col := range columns {
			record[i] = fmt.Sprintf("%v", row[col])
		}
		if err := w.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("CSV flush error: %w", err)
	}
	return buf.Bytes(), nil
}
