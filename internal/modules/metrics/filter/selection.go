package filter

import (
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// BuildSelection returns the table to query, CTE, joins, and column selections.
func BuildSelection(f Filters) (fromTable, cte, joins, selectCols, groupByCols string, args []any) {
	resourceWhere, attrWhere, args := BuildClauses(f)

	var resGroup, attrGroup []string
	for _, key := range f.GroupBy {
		if Canonical(key) != "" {
			resGroup = append(resGroup, key)
		} else {
			attrGroup = append(attrGroup, key)
		}
	}
	needRes := resourceWhere != "" || len(resGroup) > 0
	needAttr := attrWhere != "" || len(attrGroup) > 0

	if needAttr {
		fromTable = "observability.metrics"
		selectCols = bucketGrainSQL(f.StartMs, f.EndMs, f.Step) + " AS bucket_at"
		groupByCols = "bucket_at"
		for _, key := range f.GroupBy {
			alias := "`group_" + SanitizeKey(key) + "`"
			if canonical := Canonical(key); canonical != "" {
				col := ResourceColumn(canonical)
				selectCols += ", " + col + " AS " + alias
				groupByCols += ", " + alias
			} else {
				col := "attributes.`" + SanitizeKey(key) + "`::String"
				selectCols += ", " + col + " AS " + alias
				groupByCols += ", " + alias
			}
		}
		return fromTable, "", "", selectCols, groupByCols, args
	}

	// Route by effective grain, not window: an explicit fine step keeps the
	// query on the 1m tier even for long windows.
	if BucketDurationSeconds(f.StartMs, f.EndMs, f.Step) >= 3600 {
		fromTable = "observability.metrics_1h"
	} else {
		fromTable = "observability.metrics_1m"
	}
	selectCols = bucketGrainSQL(f.StartMs, f.EndMs, f.Step) + " AS bucket_at"
	groupByCols = "bucket_at"

	var ctes []string
	if needRes {
		resSel := "fingerprint"
		for _, key := range resGroup {
			resSel += ", any(" + ResourceColumn(Canonical(key)) + ") AS r_" + SanitizeKey(key)
		}
		ctes = append(ctes, `res AS (
		    SELECT `+resSel+`
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd`+resourceWhere+`
		    GROUP BY fingerprint
		)`)
		joins += " INNER JOIN res ON m.fingerprint = res.fingerprint"
		for _, key := range resGroup {
			alias := "`group_" + SanitizeKey(key) + "`"
			selectCols += ", res.r_" + SanitizeKey(key) + " AS " + alias
			groupByCols += ", " + alias
		}
	}

	if len(ctes) > 0 {
		cte = "WITH " + strings.Join(ctes, ",\n") + "\n"
	}
	return fromTable, cte, joins, selectCols, groupByCols, args
}

// BuildTagValueArms returns one SELECT arm per key (resource arms read
// metrics_resource, attribute arms read metrics_attr), whether the resource
// fingerprint CTE is needed, and the per-key named bind args.
func BuildTagValueArms(keys []string) (arms []string, needFps bool, args []any) {
	for i, key := range keys {
		label := "k" + strconv.Itoa(i)
		args = append(args, clickhouse.Named(label, key))
		if canonical := Canonical(key); canonical != "" {
			col := ResourceColumn(canonical)
			if col == "" {
				continue
			}
			needFps = true
			arms = append(arms, `
				SELECT @`+label+` AS tag_key, `+col+` AS tag_value, count() AS c
				FROM observability.metrics_resource
				PREWHERE team_id   = @teamID
				     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
				WHERE fingerprint IN fps AND `+col+` != ''
				GROUP BY tag_value`)
			continue
		}
		col := AttrColumn(key)
		arms = append(arms, `
			SELECT @`+label+` AS tag_key, `+col+` AS tag_value, count() AS c
			FROM observability.metrics_attr
			PREWHERE team_id     = @teamID
			     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
			     AND metric_name = @metricName
			WHERE `+col+` != ''
			GROUP BY tag_value`)
	}
	return arms, needFps, args
}

// BucketDurationSeconds returns the bucket duration for a step or grain.
func BucketDurationSeconds(startMs, endMs int64, step string) int64 {
	switch step {
	case "1m":
		return 60
	case "5m":
		return 300
	case "15m":
		return 900
	case "1h":
		return 3600
	case "1d":
		return 86400
	default:
		h := (endMs - startMs) / 3_600_000
		switch {
		case h <= 3:
			return 60
		case h <= 24:
			return 300
		case h <= 168:
			return 3600
		default:
			return 86400
		}
	}
}

// bucketGrainSQL returns toStartOf* fragment matching BucketDurationSeconds.
func bucketGrainSQL(startMs, endMs int64, step string) string {
	switch BucketDurationSeconds(startMs, endMs, step) {
	case 60:
		return "toStartOfMinute(timestamp)"
	case 900:
		return "toStartOfFifteenMinutes(timestamp)"
	case 3600:
		return "toStartOfHour(timestamp)"
	case 86400:
		return "toStartOfDay(timestamp)"
	default: // 300
		return "toStartOfFiveMinutes(timestamp)"
	}
}
