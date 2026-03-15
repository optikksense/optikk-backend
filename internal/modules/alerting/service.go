package alerting

import "fmt"

// AlertSummary provides a high-level overview of alerting state.
type AlertSummary struct {
	TotalRules    int   `json:"totalRules"`
	EnabledRules  int   `json:"enabledRules"`
	OpenIncidents int64 `json:"openIncidents"`
}

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// --- Alert Rules ---

func (s *Service) CreateRule(teamID, userID int64, req CreateRuleRequest) (*AlertRule, error) {
	if err := validateSeverity(req.Severity); err != nil {
		return nil, err
	}
	if err := validateConditionType(req.ConditionType); err != nil {
		return nil, err
	}
	if err := validateSignalType(req.SignalType); err != nil {
		return nil, err
	}
	if err := validateOperator(req.Operator); err != nil {
		return nil, err
	}
	return s.repo.CreateRule(teamID, userID, req)
}

func (s *Service) GetRuleByID(teamID, id int64) (*AlertRule, error) {
	return s.repo.GetRuleByID(teamID, id)
}

func (s *Service) ListRules(teamID int64) ([]AlertRule, error) {
	return s.repo.ListRules(teamID)
}

func (s *Service) UpdateRule(teamID, userID, id int64, req UpdateRuleRequest) (*AlertRule, error) {
	existing, err := s.repo.GetRuleByID(teamID, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("alert rule not found")
	}
	if existing.CreatedBy != userID {
		return nil, fmt.Errorf("only the creator can update this rule")
	}
	if req.Severity != nil {
		if err := validateSeverity(*req.Severity); err != nil {
			return nil, err
		}
	}
	if req.Operator != nil {
		if err := validateOperator(*req.Operator); err != nil {
			return nil, err
		}
	}
	return s.repo.UpdateRule(teamID, id, req)
}

func (s *Service) DeleteRule(teamID, userID, id int64) error {
	existing, err := s.repo.GetRuleByID(teamID, id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("alert rule not found")
	}
	if existing.CreatedBy != userID {
		return fmt.Errorf("only the creator can delete this rule")
	}
	return s.repo.DeleteRule(teamID, id)
}

func (s *Service) ToggleRule(teamID, userID, id int64, enabled bool) (*AlertRule, error) {
	existing, err := s.repo.GetRuleByID(teamID, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("alert rule not found")
	}
	if existing.CreatedBy != userID {
		return nil, fmt.Errorf("only the creator can toggle this rule")
	}
	return s.repo.UpdateRule(teamID, id, UpdateRuleRequest{Enabled: &enabled})
}

// --- Notification Channels ---

func (s *Service) CreateChannel(teamID, userID int64, req CreateChannelRequest) (*NotificationChannel, error) {
	if err := validateChannelType(req.ChannelType); err != nil {
		return nil, err
	}
	return s.repo.CreateChannel(teamID, userID, req)
}

func (s *Service) GetChannelByID(teamID, id int64) (*NotificationChannel, error) {
	return s.repo.GetChannelByID(teamID, id)
}

func (s *Service) ListChannels(teamID int64) ([]NotificationChannel, error) {
	return s.repo.ListChannels(teamID)
}

func (s *Service) UpdateChannel(teamID, userID, id int64, req UpdateChannelRequest) (*NotificationChannel, error) {
	existing, err := s.repo.GetChannelByID(teamID, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("notification channel not found")
	}
	if existing.CreatedBy != userID {
		return nil, fmt.Errorf("only the creator can update this channel")
	}
	return s.repo.UpdateChannel(teamID, id, req)
}

func (s *Service) DeleteChannel(teamID, userID, id int64) error {
	existing, err := s.repo.GetChannelByID(teamID, id)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("notification channel not found")
	}
	if existing.CreatedBy != userID {
		return fmt.Errorf("only the creator can delete this channel")
	}
	return s.repo.DeleteChannel(teamID, id)
}

// --- Incidents ---

func (s *Service) ListIncidents(teamID int64, status string, limit int) ([]Incident, error) {
	return s.repo.ListIncidents(teamID, status, limit)
}

func (s *Service) GetIncidentByID(teamID, id int64) (*Incident, error) {
	return s.repo.GetIncidentByID(teamID, id)
}

func (s *Service) UpdateIncidentStatus(teamID, userID, id int64, action IncidentAction) (*Incident, error) {
	existing, err := s.repo.GetIncidentByID(teamID, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("incident not found")
	}
	if err := validateStatusTransition(existing.Status, action.Status); err != nil {
		return nil, err
	}
	return s.repo.UpdateIncidentStatus(teamID, id, action.Status, userID)
}

// --- Summary ---

func (s *Service) GetAlertSummary(teamID int64) (*AlertSummary, error) {
	rules, err := s.repo.ListRules(teamID)
	if err != nil {
		return nil, err
	}
	enabledCount := 0
	for _, r := range rules {
		if r.Enabled {
			enabledCount++
		}
	}
	openIncidents, err := s.repo.CountOpenIncidents(teamID)
	if err != nil {
		return nil, err
	}
	return &AlertSummary{
		TotalRules:    len(rules),
		EnabledRules:  enabledCount,
		OpenIncidents: openIncidents,
	}, nil
}

// --- Validators ---

func validateSeverity(s string) error {
	switch s {
	case "critical", "warning", "info":
		return nil
	}
	return fmt.Errorf("invalid severity: %q (must be critical, warning, or info)", s)
}

func validateConditionType(ct string) error {
	switch ct {
	case "threshold", "absence":
		return nil
	}
	return fmt.Errorf("invalid conditionType: %q (must be threshold or absence)", ct)
}

func validateSignalType(st string) error {
	switch st {
	case "metric", "log", "span":
		return nil
	}
	return fmt.Errorf("invalid signalType: %q (must be metric, log, or span)", st)
}

func validateOperator(op string) error {
	switch op {
	case "gt", "gte", "lt", "lte", "eq":
		return nil
	}
	return fmt.Errorf("invalid operator: %q (must be gt, gte, lt, lte, or eq)", op)
}

func validateChannelType(ct string) error {
	switch ct {
	case "slack", "webhook", "email":
		return nil
	}
	return fmt.Errorf("invalid channelType: %q (must be slack, webhook, or email)", ct)
}

func validateStatusTransition(current, next string) error {
	switch {
	case current == "open" && next == "acknowledged":
		return nil
	case current == "open" && next == "resolved":
		return nil
	case current == "acknowledged" && next == "resolved":
		return nil
	}
	return fmt.Errorf("invalid status transition: %s -> %s", current, next)
}
