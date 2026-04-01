package queryparser

import (
	"testing"
)

func TestParseEmpty(t *testing.T) {
	node, err := Parse("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node != nil {
		t.Fatalf("expected nil node for empty input, got %T", node)
	}
}

func TestParseFieldValue(t *testing.T) {
	node, err := Parse("service:web-store")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fm, ok := node.(*FieldMatch)
	if !ok {
		t.Fatalf("expected FieldMatch, got %T", node)
	}
	if fm.Field != "service" || fm.Value != "web-store" {
		t.Fatalf("got field=%q value=%q", fm.Field, fm.Value)
	}
}

func TestParseQuotedFreeText(t *testing.T) {
	node, err := Parse(`"hello world"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ft, ok := node.(*FreeText)
	if !ok {
		t.Fatalf("expected FreeText, got %T", node)
	}
	if ft.Text != "hello world" {
		t.Fatalf("got text=%q", ft.Text)
	}
}

func TestParseImplicitAnd(t *testing.T) {
	node, err := Parse("service:web status:error")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	and, ok := node.(*AndNode)
	if !ok {
		t.Fatalf("expected AndNode, got %T", node)
	}
	if len(and.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(and.Children))
	}
}

func TestParseExplicitAnd(t *testing.T) {
	node, err := Parse("service:web AND status:error")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	and, ok := node.(*AndNode)
	if !ok {
		t.Fatalf("expected AndNode, got %T", node)
	}
	if len(and.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(and.Children))
	}
}

func TestParseOr(t *testing.T) {
	node, err := Parse("service:web OR service:api")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	or, ok := node.(*OrNode)
	if !ok {
		t.Fatalf("expected OrNode, got %T", node)
	}
	if len(or.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(or.Children))
	}
}

func TestParseNegation(t *testing.T) {
	node, err := Parse("-service:debug")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	not, ok := node.(*NotNode)
	if !ok {
		t.Fatalf("expected NotNode, got %T", node)
	}
	fm, ok := not.Child.(*FieldMatch)
	if !ok {
		t.Fatalf("expected FieldMatch child, got %T", not.Child)
	}
	if fm.Field != "service" || fm.Value != "debug" {
		t.Fatalf("got field=%q value=%q", fm.Field, fm.Value)
	}
}

func TestParseRange(t *testing.T) {
	node, err := Parse("@http.status_code:[400 TO 499]")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rm, ok := node.(*RangeMatch)
	if !ok {
		t.Fatalf("expected RangeMatch, got %T", node)
	}
	if rm.Field != "@http.status_code" || rm.Low != "400" || rm.High != "499" {
		t.Fatalf("got field=%q low=%q high=%q", rm.Field, rm.Low, rm.High)
	}
}

func TestParseComparison(t *testing.T) {
	node, err := Parse("@duration:>1000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cm, ok := node.(*ComparisonMatch)
	if !ok {
		t.Fatalf("expected ComparisonMatch, got %T", node)
	}
	if cm.Field != "@duration" || cm.Op != OpGT || cm.Value != "1000" {
		t.Fatalf("got field=%q op=%d value=%q", cm.Field, cm.Op, cm.Value)
	}
}

func TestParseComparisonGTE(t *testing.T) {
	node, err := Parse("@duration:>=500")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cm, ok := node.(*ComparisonMatch)
	if !ok {
		t.Fatalf("expected ComparisonMatch, got %T", node)
	}
	if cm.Op != OpGTE || cm.Value != "500" {
		t.Fatalf("got op=%d value=%q", cm.Op, cm.Value)
	}
}

func TestParseWildcard(t *testing.T) {
	node, err := Parse("service:web*")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fm, ok := node.(*FieldMatch)
	if !ok {
		t.Fatalf("expected FieldMatch, got %T", node)
	}
	if fm.Value != "web*" {
		t.Fatalf("got value=%q", fm.Value)
	}
}

func TestParseExists(t *testing.T) {
	node, err := Parse("@http.route:*")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	em, ok := node.(*ExistsMatch)
	if !ok {
		t.Fatalf("expected ExistsMatch, got %T", node)
	}
	if em.Field != "@http.route" {
		t.Fatalf("got field=%q", em.Field)
	}
}

func TestParseGroupedExpression(t *testing.T) {
	node, err := Parse("(service:web OR service:api) AND status:error")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	and, ok := node.(*AndNode)
	if !ok {
		t.Fatalf("expected AndNode, got %T", node)
	}
	if len(and.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(and.Children))
	}
	or, ok := and.Children[0].(*OrNode)
	if !ok {
		t.Fatalf("expected OrNode as first child, got %T", and.Children[0])
	}
	if len(or.Children) != 2 {
		t.Fatalf("expected 2 OR children, got %d", len(or.Children))
	}
}

func TestParseCustomAttribute(t *testing.T) {
	node, err := Parse("@user.id:abc123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fm, ok := node.(*FieldMatch)
	if !ok {
		t.Fatalf("expected FieldMatch, got %T", node)
	}
	if fm.Field != "@user.id" || fm.Value != "abc123" {
		t.Fatalf("got field=%q value=%q", fm.Field, fm.Value)
	}
}

func TestParseFreeTextWord(t *testing.T) {
	node, err := Parse("timeout")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ft, ok := node.(*FreeText)
	if !ok {
		t.Fatalf("expected FreeText, got %T", node)
	}
	if ft.Text != "timeout" {
		t.Fatalf("got text=%q", ft.Text)
	}
}

func TestParseComplexQuery(t *testing.T) {
	// service:web AND (status:error OR status:warn) AND -host:debug @http.status_code:[400 TO 499]
	node, err := Parse(`service:web AND (status:error OR status:warn) AND -host:debug @http.status_code:[400 TO 499]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	and, ok := node.(*AndNode)
	if !ok {
		t.Fatalf("expected AndNode, got %T", node)
	}
	if len(and.Children) != 4 {
		t.Fatalf("expected 4 children, got %d", len(and.Children))
	}
}

// --- Compile tests ---

func TestCompileFieldMatch(t *testing.T) {
	node, _ := Parse("service:web-store")
	result, err := Compile(node, LogsSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "service = ?" {
		t.Fatalf("got where=%q", result.Where)
	}
	if len(result.Args) != 1 || result.Args[0] != "web-store" {
		t.Fatalf("got args=%v", result.Args)
	}
}

func TestCompileWildcard(t *testing.T) {
	node, _ := Parse("service:web*")
	result, err := Compile(node, LogsSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "service LIKE ?" {
		t.Fatalf("got where=%q", result.Where)
	}
	if len(result.Args) != 1 || result.Args[0] != "web%" {
		t.Fatalf("got args=%v", result.Args)
	}
}

func TestCompileNegation(t *testing.T) {
	node, _ := Parse("-service:debug")
	result, err := Compile(node, LogsSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "NOT service = ?" {
		t.Fatalf("got where=%q", result.Where)
	}
}

func TestCompileAnd(t *testing.T) {
	node, _ := Parse("service:web status:error")
	result, err := Compile(node, LogsSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "(service = ? AND severity_text = ?)" {
		t.Fatalf("got where=%q", result.Where)
	}
	if len(result.Args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(result.Args))
	}
}

func TestCompileOr(t *testing.T) {
	node, _ := Parse("service:web OR service:api")
	result, err := Compile(node, LogsSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "(service = ? OR service = ?)" {
		t.Fatalf("got where=%q", result.Where)
	}
}

func TestCompileRange(t *testing.T) {
	node, _ := Parse("@http.status_code:[400 TO 499]")
	result, err := Compile(node, LogsSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "attributes_string['http.status_code'] BETWEEN ? AND ?" {
		t.Fatalf("got where=%q", result.Where)
	}
}

func TestCompileFreeText(t *testing.T) {
	node, _ := Parse("timeout")
	result, err := Compile(node, LogsSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "positionCaseInsensitive(body, ?) > 0" {
		t.Fatalf("got where=%q", result.Where)
	}
}

func TestCompileTracesSchema(t *testing.T) {
	node, _ := Parse("service:payment AND http.method:POST")
	result, err := Compile(node, TracesSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "(s.service_name = ? AND s.http_method = ?)" {
		t.Fatalf("got where=%q", result.Where)
	}
}

func TestCompileTracesCustomAttr(t *testing.T) {
	node, _ := Parse("@user.id:abc123")
	result, err := Compile(node, TracesSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "JSONExtractString(s.attributes, 'user.id') = ?"
	if result.Where != expected {
		t.Fatalf("got where=%q, expected=%q", result.Where, expected)
	}
}

func TestCompileTracesFreeText(t *testing.T) {
	node, _ := Parse("timeout")
	result, err := Compile(node, TracesSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "(positionCaseInsensitive(s.trace_id, ?) > 0 OR positionCaseInsensitive(s.service_name, ?) > 0 OR positionCaseInsensitive(s.name, ?) > 0 OR positionCaseInsensitive(s.status_message, ?) > 0)"
	if result.Where != expected {
		t.Fatalf("got where=%q", result.Where)
	}
}

func TestCompileExists(t *testing.T) {
	node, _ := Parse("@http.route:*")
	result, err := Compile(node, LogsSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "attributes_string['http.route'] != ''" {
		t.Fatalf("got where=%q", result.Where)
	}
}

func TestCompileComparison(t *testing.T) {
	node, _ := Parse("@duration:>1000")
	result, err := Compile(node, TracesSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "JSONExtractString(s.attributes, 'duration') > ?"
	if result.Where != expected {
		t.Fatalf("got where=%q", result.Where)
	}
}

func TestCompileNil(t *testing.T) {
	result, err := Compile(nil, LogsSchema{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Where != "" {
		t.Fatalf("expected empty where, got %q", result.Where)
	}
}

func TestCompileUnknownField(t *testing.T) {
	node, _ := Parse("unknownfield:value")
	_, err := Compile(node, LogsSchema{})
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}

func TestWildcardToLike(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"web*", "web%"},
		{"api-?", "api-_"},
		{"*store*", "%store%"},
		{"exact", "exact"},
		{"has%percent", `has\%percent`},
		{"has_under", `has\_under`},
	}
	for _, tt := range tests {
		got := wildcardToLike(tt.input)
		if got != tt.expected {
			t.Errorf("wildcardToLike(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
