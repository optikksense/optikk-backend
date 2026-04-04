package exprparser

import (
	"fmt"
	"strings"
	"unicode"
)

// Evaluate parses and evaluates a simple arithmetic expression.
// It supports +, -, *, / operators, parentheses, and single-letter variables.
// The resolve function is used to look up the value of a variable.
func Evaluate(expr string, resolve func(string) float64) (float64, error) {
	p := &exprParser{
		input:   strings.TrimSpace(expr),
		pos:     0,
		resolve: resolve,
	}
	val := p.parseExpr()

	// Check for trailing junk.
	p.skipSpaces()
	if p.pos < len(p.input) && p.err == nil {
		p.err = fmt.Errorf("unexpected character at position %d: %q", p.pos, string(p.input[p.pos]))
	}

	if p.err != nil {
		return 0, p.err
	}
	return val, nil
}

// ExtractVariables returns a unique list of single-letter variable references (a-z) found in the expression.
func ExtractVariables(expr string) []string {
	seen := map[string]bool{}
	var refs []string
	for _, r := range expr {
		if unicode.IsLetter(r) && r >= 'a' && r <= 'z' {
			s := string(r)
			if !seen[s] {
				seen[s] = true
				refs = append(refs, s)
			}
		}
	}
	return refs
}

type exprParser struct {
	input   string
	pos     int
	resolve func(string) float64
	err     error
}

func (p *exprParser) parseExpr() float64 {
	result := p.parseTerm()
	for p.pos < len(p.input) {
		p.skipSpaces()
		if p.pos >= len(p.input) {
			break
		}
		op := p.input[p.pos]
		if op != '+' && op != '-' {
			break
		}
		p.pos++
		right := p.parseTerm()
		if op == '+' {
			result += right
		} else {
			result -= right
		}
	}
	return result
}

func (p *exprParser) parseTerm() float64 {
	result := p.parseFactor()
	for p.pos < len(p.input) {
		p.skipSpaces()
		if p.pos >= len(p.input) {
			break
		}
		op := p.input[p.pos]
		if op != '*' && op != '/' {
			break
		}
		p.pos++
		right := p.parseFactor()
		if op == '*' {
			result *= right
		} else {
			if right == 0 {
				return 0
			}
			result /= right
		}
	}
	return result
}

func (p *exprParser) parseFactor() float64 {
	p.skipSpaces()
	if p.pos >= len(p.input) {
		return 0
	}

	// Parenthesized sub-expression.
	if p.input[p.pos] == '(' {
		p.pos++
		val := p.parseExpr()
		p.skipSpaces()
		if p.pos < len(p.input) && p.input[p.pos] == ')' {
			p.pos++
		}
		return val
	}

	// Number literal.
	if (p.input[p.pos] >= '0' && p.input[p.pos] <= '9') || p.input[p.pos] == '.' {
		return p.parseNumber()
	}

	// Single-letter variable reference (a-z).
	if p.input[p.pos] >= 'a' && p.input[p.pos] <= 'z' {
		ref := string(p.input[p.pos])
		p.pos++
		return p.resolve(ref)
	}

	p.err = fmt.Errorf("unexpected character at position %d: %q", p.pos, string(p.input[p.pos]))
	return 0
}

func (p *exprParser) parseNumber() float64 {
	start := p.pos
	for p.pos < len(p.input) && ((p.input[p.pos] >= '0' && p.input[p.pos] <= '9') || p.input[p.pos] == '.') {
		p.pos++
	}
	var val float64
	_, _ = fmt.Sscanf(p.input[start:p.pos], "%f", &val)
	return val
}

func (p *exprParser) skipSpaces() {
	for p.pos < len(p.input) && p.input[p.pos] == ' ' {
		p.pos++
	}
}
