package alerting

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
)

// DefaultNotifyTemplate is used when a rule doesn't specify its own template.
// Kept small and Go-template-safe rather than pulling in a Handlebars dep —
// the surface area is tiny and the FE editor can still preview via /test.
const DefaultNotifyTemplate = `{{.RuleName}} ({{.Severity}}) — state {{.State}}
{{range $k, $v := .Values}}  {{$k}}: {{printf "%.3f" $v}}{{"\n"}}{{end}}
Threshold: {{printf "%.3f" .Threshold}}
{{if .DeployHint}}Deploy: {{.DeployHint}}{{end}}
Open in Optik: {{.DeepLinkURL}}`

// TemplateData is the struct bound to notify templates.
type TemplateData struct {
	RuleName    string
	Severity    string
	State       string
	Values      map[string]float64
	Threshold   float64
	DeployHint  string
	DeepLinkURL string
	Tags        map[string]string
}

// RenderTemplate renders tmpl (or DefaultNotifyTemplate when empty) with data.
func RenderTemplate(tmpl string, data TemplateData) (string, error) {
	src := tmpl
	if strings.TrimSpace(src) == "" {
		src = DefaultNotifyTemplate
	}
	t, err := template.New("notify").Parse(src)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return buf.String(), nil
}
