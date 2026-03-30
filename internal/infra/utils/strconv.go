package utils

import "strconv"

func ToInt64(s string, fallback int64) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

func ToString(v int64) string {
	return strconv.FormatInt(v, 10)
}
