package helpers

import "time"

func ResolveRange(startPtr, endPtr *int64, defaultRangeMs int64) (int64, int64) {
	end := time.Now().UnixMilli()
	if endPtr != nil {
		end = *endPtr
	}
	start := end - defaultRangeMs
	if startPtr != nil {
		start = *startPtr
	}
	return start, end
}

func ParseMillis(v int64) time.Time {
	return time.UnixMilli(v).UTC()
}
