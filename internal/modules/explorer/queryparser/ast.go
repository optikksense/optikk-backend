package queryparser

// Node is the interface all AST nodes implement.
type Node interface {
	nodeType() string
}

// AndNode combines two or more child nodes with logical AND.
type AndNode struct {
	Children []Node
}

func (n *AndNode) nodeType() string { return "and" }

// OrNode combines two or more child nodes with logical OR.
type OrNode struct {
	Children []Node
}

func (n *OrNode) nodeType() string { return "or" }

// NotNode negates a child node.
type NotNode struct {
	Child Node
}

func (n *NotNode) nodeType() string { return "not" }

// FieldMatch matches a specific field against a value.
// Field is the user-visible name (e.g. "service", "@http.route").
// Value may contain wildcards (* and ?).
type FieldMatch struct {
	Field string
	Value string
}

func (n *FieldMatch) nodeType() string { return "field" }

// ComparisonMatch matches a field with a comparison operator.
type ComparisonMatch struct {
	Field string
	Op    ComparisonOp
	Value string
}

func (n *ComparisonMatch) nodeType() string { return "comparison" }

// ComparisonOp represents a comparison operator.
type ComparisonOp int

const (
	OpGT ComparisonOp = iota
	OpGTE
	OpLT
	OpLTE
)

// RangeMatch matches a field within a numeric/string range [low TO high].
type RangeMatch struct {
	Field string
	Low   string
	High  string
}

func (n *RangeMatch) nodeType() string { return "range" }

// FreeText is an unstructured text search term or quoted phrase.
type FreeText struct {
	Text string
}

func (n *FreeText) nodeType() string { return "freetext" }

// ExistsMatch tests whether a field exists (has any value).
type ExistsMatch struct {
	Field string
}

func (n *ExistsMatch) nodeType() string { return "exists" }
