// Package template renders a notification message body against a Payload
// derived from an evaluation. It supports the small mustache subset called out
// in the Datadog-style design: scalar variables ({{value}}, {{threshold}},
// {{service.name}}) and section blocks for status arms ({{#is_alert}}…
// {{/is_alert}}). Single responsibility: convert a template body + variables
// into a finished string. No external dep.
package template

import (
	"fmt"
	"strings"
)

// Vars carries the substitution map and the three status booleans the section
// blocks read. Keys that aren't found render as the empty string — matches
// mustache "missing key" semantics so missing context never breaks delivery.
type Vars struct {
	Values     map[string]string
	IsAlert    bool
	IsWarning  bool
	IsRecovery bool
}

// Render evaluates body against v. Unknown sections render as empty; the
// renderer is single-pass and never panics on malformed input — it returns
// what it could and leaves the rest as-is.
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
			// section markers leak through unchanged so the caller can detect
			// templates that reference unhandled sections, but in practice
			// renderSections has stripped these already.
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
