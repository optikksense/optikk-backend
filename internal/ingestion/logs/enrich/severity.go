package enrich

import "strings"

// BucketFor maps an OTLP severity_text and severity_number pair to the 0..5
// severity bucket used by the CH `severity_bucket` MATERIALIZED column on
// observability.logs_v2. Keeping this logic in Go (mirrored from the CH
// multiIf expression) lets the ingest pipeline emit the bucket on the wire so
// explorer queries that group by severity do not pay the materialize cost.
//
//	0  TRACE / UNSET  severity_number <= 4
//	1  DEBUG          severity_number 5..8
//	2  INFO           severity_number 9..12
//	3  WARN           severity_number 13..16
//	4  ERROR          severity_number 17..20
//	5  FATAL          severity_number >= 21
func BucketFor(sevText string, sevNum int32) uint8 {
	if sevNum > 0 {
		return bucketFromNumber(sevNum)
	}
	switch strings.ToUpper(strings.TrimSpace(sevText)) {
	case "FATAL", "CRITICAL", "CRIT", "EMERGENCY", "ALERT":
		return 5
	case "ERROR", "ERR", "SEVERE":
		return 4
	case "WARN", "WARNING":
		return 3
	case "INFO", "INFORMATION", "NOTICE":
		return 2
	case "DEBUG":
		return 1
	case "TRACE", "FINE", "FINER", "FINEST":
		return 0
	}
	return 0
}

func bucketFromNumber(n int32) uint8 {
	switch {
	case n >= 21:
		return 5
	case n >= 17:
		return 4
	case n >= 13:
		return 3
	case n >= 9:
		return 2
	case n >= 5:
		return 1
	default:
		return 0
	}
}
