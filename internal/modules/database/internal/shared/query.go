package shared

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Filters struct {
	DBSystem   []string
	Collection []string
	Namespace  []string
	Server     []string
}

func FilterClauses(f Filters) (string, []any) {
	var sb strings.Builder
	var args []any

	appendIn := func(attr string, prefix string, values []string) {
		if len(values) == 0 {
			return
		}
		clause, namedArgs := dbutil.NamedInClause(prefix, values)
		sb.WriteString(fmt.Sprintf(" AND %s IN %s", AttrString(attr), clause))
		for key, value := range namedArgs {
			args = append(args, clickhouse.Named(key, value))
		}
	}

	appendIn(AttrDBSystem, "dbSystem", f.DBSystem)
	appendIn(AttrDBCollectionName, "dbCollection", f.Collection)
	appendIn(AttrDBNamespace, "dbNamespace", f.Namespace)
	appendIn(AttrServerAddress, "dbServer", f.Server)

	return sb.String(), args
}

func BaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func ScaleToMs(v *float64) *float64 {
	if v == nil {
		return nil
	}
	ms := *v * 1000.0
	return &ms
}

func BucketWidthSeconds(startMs, endMs int64) float64 {
	hours := float64(endMs-startMs) / 3_600_000.0
	switch {
	case hours <= 3:
		return 60
	case hours <= 24:
		return 300
	case hours <= 168:
		return 3600
	default:
		return 86400
	}
}
