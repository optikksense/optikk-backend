// Package notifications owns channels, policies, templates, and the static
// integrations catalog for the alerting platform. Channel rows back monitor
// notify lists; templates carry custom message bodies; policies are CRUD-only
// in v1 (the evaluator does not yet consult them).
package notifications

import (
	"encoding/json"
)

// CreateChannelRequest is the wire shape for POST /notifications/channels.
type CreateChannelRequest struct {
	Type   string          `json:"type" binding:"required"`
	Name   string          `json:"name" binding:"required"`
	Config json.RawMessage `json:"config"`
}

// UpdateChannelRequest mirrors Create for PUT.
type UpdateChannelRequest = CreateChannelRequest

// CreatePolicyRequest is POST /notifications/policies.
type CreatePolicyRequest struct {
	Name     string          `json:"name" binding:"required"`
	MatchDSL string          `json:"match_dsl" binding:"required"`
	Actions  json.RawMessage `json:"actions"`
	Enabled  *bool           `json:"enabled,omitempty"`
	Position *int            `json:"position,omitempty"`
}

// UpdatePolicyRequest mirrors CreatePolicyRequest for PUT.
type UpdatePolicyRequest = CreatePolicyRequest

// CreateTemplateRequest is POST /notifications/templates.
type CreateTemplateRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	Body        string `json:"body" binding:"required"`
}

// UpdateTemplateRequest mirrors CreateTemplateRequest for PUT.
type UpdateTemplateRequest = CreateTemplateRequest
