package queryparser

import "strings"

// LogsSchema resolves field names for the ClickHouse logs table.
type LogsSchema struct{}

var logsFieldMap = map[string]FieldInfo{
	"service":     {Column: "service", Type: FieldString},
	"host":        {Column: "host", Type: FieldString},
	"pod":         {Column: "pod", Type: FieldString},
	"container":   {Column: "container", Type: FieldString},
	"environment": {Column: "environment", Type: FieldString},
	"status":      {Column: "severity_text", Type: FieldString},
	"level":       {Column: "severity_text", Type: FieldString},
	"severity":    {Column: "severity_text", Type: FieldString},
	"scope":       {Column: "scope_name", Type: FieldString},
	"scope_name":  {Column: "scope_name", Type: FieldString},
	"logger":      {Column: "scope_name", Type: FieldString},
	"message":     {Column: "body", Type: FieldString},
	"body":        {Column: "body", Type: FieldString},
	"trace_id":    {Column: "trace_id", Type: FieldString},
	"span_id":     {Column: "span_id", Type: FieldString},
}

func (LogsSchema) Resolve(field string) (FieldInfo, bool) {
	lower := strings.ToLower(field)

	if info, ok := logsFieldMap[lower]; ok {
		return info, true
	}

	// Custom attributes: @key maps to attribute maps.
	if strings.HasPrefix(field, "@") {
		attrKey := field[1:]
		// Try string attributes by default. The compiler produces a string comparison;
		// numeric/bool attributes can be accessed via @num. or @bool. prefix.
		if strings.HasPrefix(attrKey, "num.") {
			return FieldInfo{
				Column: "attributes_number['" + escapeSingleQuote(attrKey[4:]) + "']",
				Type:   FieldNumber,
			}, true
		}
		if strings.HasPrefix(attrKey, "bool.") {
			return FieldInfo{
				Column: "attributes_bool['" + escapeSingleQuote(attrKey[5:]) + "']",
				Type:   FieldBool,
			}, true
		}
		return FieldInfo{
			Column: "attributes_string['" + escapeSingleQuote(attrKey) + "']",
			Type:   FieldString,
		}, true
	}

	return FieldInfo{}, false
}

func (LogsSchema) FreeTextColumns() []string {
	return []string{"body"}
}

func (LogsSchema) TableAlias() string { return "" }

func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}
