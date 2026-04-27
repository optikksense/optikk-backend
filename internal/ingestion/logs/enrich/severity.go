package enrich

import "strings"

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
