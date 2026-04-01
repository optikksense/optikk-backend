package queryparser

// FieldInfo describes how a user-visible field maps to ClickHouse.
type FieldInfo struct {
	// Column is the ClickHouse column expression (e.g. "s.service_name").
	Column string
	// Type is the ClickHouse data type category.
	Type FieldType
}

// FieldType classifies a field for query compilation.
type FieldType int

const (
	FieldString FieldType = iota
	FieldNumber
	FieldBool
)

// SchemaResolver maps user-visible field names to ClickHouse column info.
// It handles both well-known fields (service, status) and custom attributes (@prefix).
type SchemaResolver interface {
	// Resolve returns FieldInfo for a user-visible field name.
	// Returns ok=false if the field is unknown.
	Resolve(field string) (FieldInfo, bool)

	// FreeTextColumns returns the ClickHouse columns to search for free-text queries.
	FreeTextColumns() []string

	// TableAlias returns the table alias used in queries (e.g. "" for logs, "s" for spans).
	TableAlias() string
}
