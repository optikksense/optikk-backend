package shared

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

type Filters struct {
	DBSystem   []string
	Collection []string
	Namespace  []string
	Server     []string
}

func FilterClauses(f Filters) (frag string, args []any) {

	appendIn := func(attr string, prefix string, values []string) {
		if len(values) == 0 {
			return
		}
		frag += " AND " + AttrString(attr) + " IN @" + prefix
		args = append(args, clickhouse.Named(prefix, values))
	}

	appendIn(AttrDBSystem, "dbSystem", f.DBSystem)
	appendIn(AttrDBCollectionName, "dbCollection", f.Collection)
	appendIn(AttrDBNamespace, "dbNamespace", f.Namespace)
	appendIn(AttrServerAddress, "dbServer", f.Server)

	return frag, args
}

func BaseParams(teamID int64, startMs, endMs int64) []any {
	return dbutil.SimpleBaseParams(teamID, startMs, endMs)
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
