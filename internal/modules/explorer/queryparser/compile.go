package queryparser

import (
	"fmt"
	"strings"
)

// CompileResult holds the ClickHouse WHERE clause fragment and arguments.
type CompileResult struct {
	Where string
	Args  []any
}

// Compile converts a parsed AST into a ClickHouse WHERE clause fragment.
// Returns an empty result if node is nil.
func Compile(node Node, schema SchemaResolver) (CompileResult, error) {
	if node == nil {
		return CompileResult{}, nil
	}
	c := &compiler{schema: schema}
	frag, err := c.compile(node)
	if err != nil {
		return CompileResult{}, err
	}
	return CompileResult{Where: frag, Args: c.args}, nil
}

type compiler struct {
	schema SchemaResolver
	args   []any
}

func (c *compiler) addArg(v any) string {
	c.args = append(c.args, v)
	return "?"
}

func (c *compiler) compile(node Node) (string, error) {
	switch n := node.(type) {
	case *AndNode:
		return c.compileAnd(n)
	case *OrNode:
		return c.compileOr(n)
	case *NotNode:
		return c.compileNot(n)
	case *FieldMatch:
		return c.compileField(n)
	case *ComparisonMatch:
		return c.compileComparison(n)
	case *RangeMatch:
		return c.compileRange(n)
	case *FreeText:
		return c.compileFreeText(n)
	case *ExistsMatch:
		return c.compileExists(n)
	default:
		return "", fmt.Errorf("unknown node type: %T", node)
	}
}

func (c *compiler) compileAnd(n *AndNode) (string, error) {
	parts := make([]string, 0, len(n.Children))
	for _, child := range n.Children {
		frag, err := c.compile(child)
		if err != nil {
			return "", err
		}
		parts = append(parts, frag)
	}
	return "(" + strings.Join(parts, " AND ") + ")", nil
}

func (c *compiler) compileOr(n *OrNode) (string, error) {
	parts := make([]string, 0, len(n.Children))
	for _, child := range n.Children {
		frag, err := c.compile(child)
		if err != nil {
			return "", err
		}
		parts = append(parts, frag)
	}
	return "(" + strings.Join(parts, " OR ") + ")", nil
}

func (c *compiler) compileNot(n *NotNode) (string, error) {
	frag, err := c.compile(n.Child)
	if err != nil {
		return "", err
	}
	return "NOT " + frag, nil
}

func (c *compiler) compileField(n *FieldMatch) (string, error) {
	info, ok := c.schema.Resolve(n.Field)
	if !ok {
		return "", fmt.Errorf("unknown field: %q", n.Field)
	}

	// Wildcard matching.
	if strings.ContainsAny(n.Value, "*?") {
		pattern := wildcardToLike(n.Value)
		return info.Column + " LIKE " + c.addArg(pattern), nil
	}

	return info.Column + " = " + c.addArg(n.Value), nil
}

func (c *compiler) compileComparison(n *ComparisonMatch) (string, error) {
	info, ok := c.schema.Resolve(n.Field)
	if !ok {
		return "", fmt.Errorf("unknown field: %q", n.Field)
	}

	var opStr string
	switch n.Op {
	case OpGT:
		opStr = ">"
	case OpGTE:
		opStr = ">="
	case OpLT:
		opStr = "<"
	case OpLTE:
		opStr = "<="
	}

	return info.Column + " " + opStr + " " + c.addArg(n.Value), nil
}

func (c *compiler) compileRange(n *RangeMatch) (string, error) {
	info, ok := c.schema.Resolve(n.Field)
	if !ok {
		return "", fmt.Errorf("unknown field: %q", n.Field)
	}

	return info.Column + " BETWEEN " + c.addArg(n.Low) + " AND " + c.addArg(n.High), nil
}

func (c *compiler) compileFreeText(n *FreeText) (string, error) {
	cols := c.schema.FreeTextColumns()
	if len(cols) == 0 {
		return "1=1", nil
	}

	parts := make([]string, 0, len(cols))
	for _, col := range cols {
		parts = append(parts, "positionCaseInsensitive("+col+", "+c.addArg(n.Text)+") > 0")
	}

	if len(parts) == 1 {
		return parts[0], nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", nil
}

func (c *compiler) compileExists(n *ExistsMatch) (string, error) {
	info, ok := c.schema.Resolve(n.Field)
	if !ok {
		return "", fmt.Errorf("unknown field: %q", n.Field)
	}

	switch info.Type {
	case FieldString:
		return info.Column + " != ''", nil
	case FieldNumber:
		return info.Column + " != 0", nil
	case FieldBool:
		return info.Column + " = true", nil
	default:
		return info.Column + " != ''", nil
	}
}

// wildcardToLike converts Datadog-style wildcards to SQL LIKE patterns.
// * -> %, ? -> _
func wildcardToLike(pattern string) string {
	var sb strings.Builder
	for _, ch := range pattern {
		switch ch {
		case '*':
			sb.WriteByte('%')
		case '?':
			sb.WriteByte('_')
		case '%':
			sb.WriteString(`\%`)
		case '_':
			sb.WriteString(`\_`)
		default:
			sb.WriteRune(ch)
		}
	}
	return sb.String()
}
