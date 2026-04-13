package database

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// NamedInArgs builds a named-parameter IN clause and returns the SQL fragment
// plus clickhouse.Named args ready for query execution.
//
// Example:
//
//	frag, args := NamedInArgs("s.service_name", "svc", []string{"a","b"})
//	// frag = "s.service_name IN (@svc0,@svc1)"
//	// args = [clickhouse.Named("svc0","a"), clickhouse.Named("svc1","b")]
func NamedInArgs(column, prefix string, values []string) (fragment string, args []any) {
	if len(values) == 0 {
		return "", nil
	}
	placeholders := make([]string, len(values))
	args = make([]any, len(values))
	for i, v := range values {
		name := fmt.Sprintf("%s%d", prefix, i)
		placeholders[i] = "@" + name
		args[i] = clickhouse.Named(name, v)
	}
	return column + " IN (" + strings.Join(placeholders, ",") + ")", args
}
