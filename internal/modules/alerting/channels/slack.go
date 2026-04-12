package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Slack posts to a Slack incoming webhook URL.
type Slack struct {
	Client *http.Client
}

func NewSlack() *Slack {
	return &Slack{Client: &http.Client{Timeout: 10 * time.Second}}
}

func (s *Slack) Name() string { return "slack" }

func (s *Slack) Send(ctx context.Context, webhookURL string, r Rendered) error {
	if webhookURL == "" {
		return fmt.Errorf("slack: webhook url required")
	}
	payload := map[string]any{
		"text": fmt.Sprintf("*[%s] %s*\n%s", r.Severity, r.Title, r.Body),
	}
	if r.DeepLinkURL != "" {
		payload["blocks"] = []map[string]any{
			{
				"type": "section",
				"text": map[string]any{
					"type": "mrkdwn",
					"text": fmt.Sprintf("*[%s] %s*\n%s", r.Severity, r.Title, r.Body),
				},
			},
			{
				"type": "actions",
				"elements": []map[string]any{
					{"type": "button", "text": map[string]any{"type": "plain_text", "text": "Open in Optik"}, "url": r.DeepLinkURL},
				},
			},
		}
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("slack: status %d", resp.StatusCode)
	}
	return nil
}
