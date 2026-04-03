package queryparser

import (
	"fmt"
	"strings"
	"unicode"
)

// Parse parses a Datadog-style query string into an AST.
//
// Grammar (informal):
//
//	query     = orExpr
//	orExpr    = andExpr ("OR" andExpr)*
//	andExpr   = unaryExpr ("AND"? unaryExpr)*
//	unaryExpr = "-" unaryExpr | "(" orExpr ")" | atom
//	atom      = field ":" value | field ":" comparison | field ":" range | freeText
//	field     = identifier | "@" dotted-identifier
//	value     = word | quoted-string  (may contain * or ?)
//	range     = "[" low "TO" high "]"
//	comparison = (">" | ">=" | "<" | "<=") number
func Parse(input string) (Node, error) {
	p := &parser{input: input}
	p.tokenize()
	if len(p.tokens) == 0 {
		return nil, nil
	}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos < len(p.tokens) {
		return nil, fmt.Errorf("unexpected token at position %d: %q", p.pos, p.tokens[p.pos].value)
	}
	return node, nil
}

type tokenKind int

const (
	tokWord tokenKind = iota
	tokQuoted
	tokLParen
	tokRParen
	tokLBracket
	tokRBracket
	tokColon
	tokGT
	tokGTE
	tokLT
	tokLTE
	tokMinus
)

type token struct {
	kind  tokenKind
	value string
}

type parser struct {
	input  string
	tokens []token
	pos    int
}

func (p *parser) peek() *token {
	if p.pos >= len(p.tokens) {
		return nil
	}
	return &p.tokens[p.pos]
}

func (p *parser) next() *token {
	t := p.peek()
	if t != nil {
		p.pos++
	}
	return t
}

func (p *parser) expect(kind tokenKind) (*token, error) {
	t := p.next()
	if t == nil {
		return nil, fmt.Errorf("unexpected end of input, expected %d", kind)
	}
	if t.kind != kind {
		return nil, fmt.Errorf("expected token kind %d, got %q", kind, t.value)
	}
	return t, nil
}

// tokenize splits the input into tokens.
func (p *parser) tokenize() {
	s := p.input
	i := 0
	for i < len(s) {
		ch := rune(s[i])
		if unicode.IsSpace(ch) {
			i++
			continue
		}

		switch ch {
		case '(':
			p.tokens = append(p.tokens, token{kind: tokLParen, value: "("})
			i++
		case ')':
			p.tokens = append(p.tokens, token{kind: tokRParen, value: ")"})
			i++
		case '[':
			p.tokens = append(p.tokens, token{kind: tokLBracket, value: "["})
			i++
		case ']':
			p.tokens = append(p.tokens, token{kind: tokRBracket, value: "]"})
			i++
		case ':':
			p.tokens = append(p.tokens, token{kind: tokColon, value: ":"})
			i++
		case '-':
			// Distinguish negation prefix from minus within a word.
			// If the previous token is a colon or there's no previous token or the
			// previous is a logical operator or open paren, treat as negation.
			if p.isNegationContext() {
				p.tokens = append(p.tokens, token{kind: tokMinus, value: "-"})
				i++
			} else {
				// Part of a word (e.g., "web-store")
				word, end := p.readWord(i)
				p.tokens = append(p.tokens, token{kind: tokWord, value: word})
				i = end
			}
		case '>':
			if i+1 < len(s) && s[i+1] == '=' {
				p.tokens = append(p.tokens, token{kind: tokGTE, value: ">="})
				i += 2
			} else {
				p.tokens = append(p.tokens, token{kind: tokGT, value: ">"})
				i++
			}
		case '<':
			if i+1 < len(s) && s[i+1] == '=' {
				p.tokens = append(p.tokens, token{kind: tokLTE, value: "<="})
				i += 2
			} else {
				p.tokens = append(p.tokens, token{kind: tokLT, value: "<"})
				i++
			}
		case '"':
			str, end := p.readQuoted(i)
			p.tokens = append(p.tokens, token{kind: tokQuoted, value: str})
			i = end
		default:
			word, end := p.readWord(i)
			p.tokens = append(p.tokens, token{kind: tokWord, value: word})
			i = end
		}
	}
}

func (p *parser) isNegationContext() bool {
	if len(p.tokens) == 0 {
		return true
	}
	last := p.tokens[len(p.tokens)-1]
	switch last.kind {
	case tokLParen, tokMinus:
		return true
	case tokWord:
		upper := strings.ToUpper(last.value)
		return upper == "AND" || upper == "OR"
	}
	return false
}

func (p *parser) readQuoted(start int) (string, int) {
	i := start + 1 // skip opening "
	var sb strings.Builder
	for i < len(p.input) {
		ch := p.input[i]
		if ch == '\\' && i+1 < len(p.input) {
			sb.WriteByte(p.input[i+1])
			i += 2
			continue
		}
		if ch == '"' {
			i++ // skip closing "
			return sb.String(), i
		}
		sb.WriteByte(ch)
		i++
	}
	// Unterminated quote — return what we have.
	return sb.String(), i
}

func (p *parser) readWord(start int) (string, int) {
	i := start
	for i < len(p.input) {
		ch := rune(p.input[i])
		if unicode.IsSpace(ch) || ch == '(' || ch == ')' || ch == '[' || ch == ']' || ch == ':' || ch == '"' {
			break
		}
		// Stop at > < only if not part of >=, <=
		if (ch == '>' || ch == '<') && i > start {
			break
		}
		i++
	}
	return p.input[start:i], i
}

// parseOr handles: andExpr ("OR" andExpr)*
func (p *parser) parseOr() (Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	children := []Node{left}
	for {
		t := p.peek()
		if t == nil || t.kind != tokWord || strings.ToUpper(t.value) != "OR" {
			break
		}
		p.next() // consume OR
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		children = append(children, right)
	}

	if len(children) == 1 {
		return children[0], nil
	}
	return &OrNode{Children: children}, nil
}

// parseAnd handles: unaryExpr ("AND"? unaryExpr)*
// Implicit AND: two adjacent expressions without an explicit operator.
func (p *parser) parseAnd() (Node, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	children := []Node{left}
	for {
		t := p.peek()
		if t == nil || t.kind == tokRParen {
			break
		}

		// Check for explicit "OR" — belongs to the outer parseOr.
		if t.kind == tokWord && strings.ToUpper(t.value) == "OR" {
			break
		}

		// Consume optional explicit "AND".
		if t.kind == tokWord && strings.ToUpper(t.value) == "AND" {
			p.next()
		}

		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		children = append(children, right)
	}

	if len(children) == 1 {
		return children[0], nil
	}
	return &AndNode{Children: children}, nil
}

// parseUnary handles: "-" unaryExpr | "(" orExpr ")" | atom
func (p *parser) parseUnary() (Node, error) {
	t := p.peek()
	if t == nil {
		return nil, fmt.Errorf("unexpected end of input")
	}

	if t.kind == tokMinus {
		p.next()
		child, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &NotNode{Child: child}, nil
	}

	if t.kind == tokLParen {
		p.next() // consume (
		node, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		return node, nil
	}

	return p.parseAtom()
}

// parseAtom handles field:value, field:[range], field:>N, or free text.
func (p *parser) parseAtom() (Node, error) {
	t := p.next()
	if t == nil {
		return nil, fmt.Errorf("unexpected end of input")
	}

	// Check if next token is a colon — making this a field:value expression.
	if (t.kind == tokWord || t.kind == tokQuoted) && p.peek() != nil && p.peek().kind == tokColon {
		field := t.value
		p.next() // consume colon

		return p.parseFieldValue(field)
	}

	// Otherwise it's free text.
	if t.kind == tokQuoted {
		return &FreeText{Text: t.value}, nil
	}
	return &FreeText{Text: t.value}, nil
}

// parseFieldValue parses the value part after "field:".
func (p *parser) parseFieldValue(field string) (Node, error) {
	t := p.peek()
	if t == nil {
		return nil, fmt.Errorf("expected value after %q", field)
	}

	// Range: [low TO high]
	if t.kind == tokLBracket {
		return p.parseRange(field)
	}

	// Comparison: >N, >=N, <N, <=N
	if t.kind == tokGT || t.kind == tokGTE || t.kind == tokLT || t.kind == tokLTE {
		return p.parseComparison(field)
	}

	// Regular value (word or quoted).
	val := p.next()
	if val == nil {
		return nil, fmt.Errorf("expected value after %q", field)
	}

	// Check for wildcard "*" meaning "exists".
	if val.value == "*" && val.kind == tokWord {
		return &ExistsMatch{Field: field}, nil
	}

	return &FieldMatch{Field: field, Value: val.value}, nil
}

func (p *parser) parseRange(field string) (Node, error) {
	p.next() // consume [

	low := p.next()
	if low == nil {
		return nil, fmt.Errorf("expected low value in range for %q", field)
	}

	to := p.next()
	if to == nil || strings.ToUpper(to.value) != "TO" {
		return nil, fmt.Errorf("expected TO in range for %q", field)
	}

	high := p.next()
	if high == nil {
		return nil, fmt.Errorf("expected high value in range for %q", field)
	}

	if _, err := p.expect(tokRBracket); err != nil {
		return nil, fmt.Errorf("expected ] in range for %q", field)
	}

	return &RangeMatch{Field: field, Low: low.value, High: high.value}, nil
}

func (p *parser) parseComparison(field string) (Node, error) {
	opTok := p.next()
	valTok := p.next()
	if valTok == nil {
		return nil, fmt.Errorf("expected value after operator for %q", field)
	}

	var op ComparisonOp
	switch opTok.kind {
	case tokGT:
		op = OpGT
	case tokGTE:
		op = OpGTE
	case tokLT:
		op = OpLT
	case tokLTE:
		op = OpLTE
	}

	return &ComparisonMatch{Field: field, Op: op, Value: valTok.value}, nil
}
