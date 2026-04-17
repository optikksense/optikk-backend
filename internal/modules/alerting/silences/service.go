package silences

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/engine"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/rules"
)

// Service covers silence CRUD. Silences are stored inline on the owning rule
// (see shared.Rule.Silences) so persistence flows through rules.Repository.
type Service interface {
	List(ctx context.Context, teamID int64) ([]Silence, error)
	Create(ctx context.Context, teamID, userID int64, req CreateSilenceRequest) (*Silence, error)
	Update(ctx context.Context, teamID, alertID int64, silenceID string, req UpdateSilenceRequest) (*Silence, error)
	Delete(ctx context.Context, teamID, alertID int64, silenceID string) error
}

type service struct {
	repo   rules.Repository
	events engine.EventStore
}

func NewService(repo rules.Repository, events engine.EventStore) Service {
	return &service{repo: repo, events: events}
}

func (s *service) List(ctx context.Context, teamID int64) ([]Silence, error) {
	rs, err := s.repo.List(ctx, teamID)
	if err != nil {
		return nil, err
	}
	var out []Silence
	for _, r := range rs {
		out = append(out, r.Silences...)
	}
	return out, nil
}

func (s *service) Create(ctx context.Context, teamID, userID int64, req CreateSilenceRequest) (*Silence, error) {
	if req.AlertID == 0 {
		return nil, errors.New("alerting: silence requires alert_id")
	}
	rule, err := s.repo.Get(ctx, teamID, req.AlertID)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, shared.ErrRuleNotFound
	}
	sil := Silence{
		ID:         fmt.Sprintf("sil_%d", time.Now().UnixNano()),
		StartsAt:   req.StartsAt,
		EndsAt:     req.EndsAt,
		Recurrence: req.Recurrence,
		MatchTags:  req.MatchTags,
		Reason:     req.Reason,
		CreatedBy:  userID,
	}
	rule.Silences = append(rule.Silences, sil)
	if err := s.repo.Update(ctx, rule); err != nil {
		return nil, err
	}
	if s.events != nil {
		if err := s.events.WriteEvent(ctx, shared.AlertEvent{
			TeamID:  uint32(teamID), //nolint:gosec
			AlertID: rule.ID, Kind: shared.EventKindSilence, ActorUserID: userID,
		}); err != nil {
			slog.Debug("alerting: write audit event failed", slog.Any("error", err))
		}
	}
	return &sil, nil
}

func (s *service) Update(ctx context.Context, teamID int64, alertID int64, silenceID string, req UpdateSilenceRequest) (*Silence, error) {
	rule, err := s.repo.Get(ctx, teamID, alertID)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, shared.ErrRuleNotFound
	}
	var updated *Silence
	for i := range rule.Silences {
		if rule.Silences[i].ID == silenceID {
			if req.StartsAt != nil {
				rule.Silences[i].StartsAt = *req.StartsAt
			}
			if req.EndsAt != nil {
				rule.Silences[i].EndsAt = *req.EndsAt
			}
			if req.Recurrence != nil {
				rule.Silences[i].Recurrence = *req.Recurrence
			}
			if req.MatchTags != nil {
				rule.Silences[i].MatchTags = *req.MatchTags
			}
			if req.Reason != nil {
				rule.Silences[i].Reason = *req.Reason
			}
			sil := rule.Silences[i]
			updated = &sil
			break
		}
	}
	if updated == nil {
		return nil, errors.New("alerting: silence not found")
	}
	if err := s.repo.Update(ctx, rule); err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *service) Delete(ctx context.Context, teamID, alertID int64, silenceID string) error {
	rule, err := s.repo.Get(ctx, teamID, alertID)
	if err != nil {
		return err
	}
	if rule == nil {
		return shared.ErrRuleNotFound
	}
	out := rule.Silences[:0]
	for _, sil := range rule.Silences {
		if sil.ID != silenceID {
			out = append(out, sil)
		}
	}
	rule.Silences = out
	return s.repo.Update(ctx, rule)
}
