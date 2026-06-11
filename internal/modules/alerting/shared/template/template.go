// Package template renders notification message bodies against payload values.
// It supports scalar mustache variables and status section blocks.
package template

import (
	"fmt"
	"strings"
)

// Vars carries the substitution map and the three status booleans for sections.
// Missing keys render as the empty string to prevent breaking delivery.
type Vars struct {
	Values     map[string]string
	IsAlert    bool
	IsWarning  bool
	IsRecovery bool
}

// Render evaluates the body against the provided Vars.
// The renderer is single-pass and returns whatever it can process on error.
func Render(body string, v Vars) string {
	out := body
	out = renderSections(out, "is_alert", v.IsAlert)
	out = renderSections(out, "is_warning", v.IsWarning)
	out = renderSections(out, "is_recovery", v.IsRecovery)
	out = renderScalars(out, v.Values)
	return out
}

// renderSections strips or keeps the body between {{#tag}}…{{/tag}}.
func renderSections(body, tag string, keep bool) string {
	open := "{{#" + tag + "}}"
	close := "{{/" + tag + "}}"
	for {
		i := strings.Index(body, open)
		if i < 0 {
			return body
		}
		j := strings.Index(body[i:], close)
		if j < 0 {
			return body
		}
		j += i
		inner := body[i+len(open) : j]
		replacement := ""
		if keep {
			replacement = inner
		}
		body = body[:i] + replacement + body[j+len(close):]
	}
}

// renderScalars replaces {{key}} substrings with values[key].
func renderScalars(body string, values map[string]string) string {
	out := strings.Builder{}
	out.Grow(len(body))
	i := 0
	for i < len(body) {
		if i+1 < len(body) && body[i] == '{' && body[i+1] == '{' {
			end := strings.Index(body[i:], "}}")
			if end < 0 {
				out.WriteString(body[i:])
				return out.String()
			}
			key := strings.TrimSpace(body[i+2 : i+end])
			// Section markers leak through unchanged if not previously stripped
			// by renderSections.
			if strings.HasPrefix(key, "#") || strings.HasPrefix(key, "/") {
				out.WriteString(body[i : i+end+2])
				i += end + 2
				continue
			}
			if v, ok := values[key]; ok {
				out.WriteString(v)
			}
			i += end + 2
			continue
		}
		out.WriteByte(body[i])
		i++
	}
	return out.String()
}

// FormatFloat is a small helper for callers building the Values map.
func FormatFloat(v float64) string {
	return fmt.Sprintf("%g", v)
}
