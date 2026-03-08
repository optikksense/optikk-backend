package defaultconfig

import (
	"encoding/json"
	"fmt"
)

// PageMetadata describes a top-level dashboard page exposed to the frontend.
type PageMetadata struct {
	ID           string `json:"id"`
	Path         string `json:"path"`
	Label        string `json:"label"`
	Icon         string `json:"icon"`
	Group        string `json:"group"`
	Order        int    `json:"order"`
	DefaultTabID string `json:"defaultTabId"`
	Navigable    bool   `json:"navigable"`
	Title        string `json:"title,omitempty"`
	Subtitle     string `json:"subtitle,omitempty"`
}

// QuerySpec describes the backend API contract for a single component.
type QuerySpec struct {
	Method   string         `json:"method"`
	Endpoint string         `json:"endpoint"`
	Params   map[string]any `json:"params,omitempty"`
	Extra    map[string]any `json:"-"`
}

// Component describes a renderable dashboard component and its query contract.
type Component struct {
	ID           string         `json:"id"`
	ComponentKey string         `json:"componentKey"`
	Title        string         `json:"title,omitempty"`
	Layout       map[string]any `json:"layout,omitempty"`
	Order        int            `json:"order"`
	Query        QuerySpec      `json:"query,omitempty"`
	TitleIcon    string         `json:"titleIcon,omitempty"`
	Extra        map[string]any `json:"-"`
}

// TabDefinition describes a single tab and its renderable components.
type TabDefinition struct {
	ID         string      `json:"id"`
	PageID     string      `json:"pageId"`
	Label      string      `json:"label"`
	Order      int         `json:"order"`
	Components []Component `json:"components,omitempty"`
}

// PageDocument is the canonical stored form for a page override.
type PageDocument struct {
	Page PageMetadata    `json:"page"`
	Tabs []TabDefinition `json:"tabs"`
}

func (q *QuerySpec) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	q.Method = stringValue(raw["method"])
	q.Endpoint = stringValue(raw["endpoint"])
	q.Params = mapValue(raw["params"])
	delete(raw, "method")
	delete(raw, "endpoint")
	delete(raw, "params")
	if len(raw) > 0 {
		q.Extra = raw
	}
	return nil
}

func (q QuerySpec) MarshalJSON() ([]byte, error) {
	raw := map[string]any{
		"method":   q.Method,
		"endpoint": q.Endpoint,
	}
	if len(q.Params) > 0 {
		raw["params"] = q.Params
	}
	for key, value := range q.Extra {
		if _, exists := raw[key]; !exists {
			raw[key] = value
		}
	}
	return json.Marshal(raw)
}

func (c *Component) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	c.ID = stringValue(raw["id"])
	c.ComponentKey = stringValue(raw["componentKey"])
	c.Title = stringValue(raw["title"])
	c.Layout = mapValue(raw["layout"])
	c.Order = intValue(raw["order"])
	c.TitleIcon = stringValue(raw["titleIcon"])
	if queryRaw, ok := raw["query"]; ok {
		queryBytes, err := json.Marshal(queryRaw)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(queryBytes, &c.Query); err != nil {
			return fmt.Errorf("component %q: invalid query: %w", c.ID, err)
		}
	}
	delete(raw, "id")
	delete(raw, "componentKey")
	delete(raw, "title")
	delete(raw, "layout")
	delete(raw, "order")
	delete(raw, "query")
	delete(raw, "titleIcon")
	if len(raw) > 0 {
		c.Extra = raw
	}
	return nil
}

func (c Component) MarshalJSON() ([]byte, error) {
	raw := map[string]any{
		"id":           c.ID,
		"componentKey": c.ComponentKey,
		"order":        c.Order,
	}
	raw["query"] = c.Query
	if c.Title != "" {
		raw["title"] = c.Title
	}
	if len(c.Layout) > 0 {
		raw["layout"] = c.Layout
	}
	if c.TitleIcon != "" {
		raw["titleIcon"] = c.TitleIcon
	}
	for key, value := range c.Extra {
		if _, exists := raw[key]; !exists {
			raw[key] = value
		}
	}
	return json.Marshal(raw)
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprint(value)
}

func mapValue(value any) map[string]any {
	if value == nil {
		return nil
	}
	if mapped, ok := value.(map[string]any); ok {
		return mapped
	}
	return nil
}

func intValue(value any) int {
	switch typed := value.(type) {
	case nil:
		return 0
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case float32:
		return int(typed)
	default:
		return 0
	}
}
