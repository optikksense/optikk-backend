package enrich

// StatusBucket maps a span status_code (0=UNSET, 1=OK, 2=ERROR) plus
// http.response.status_code to a compact byte used for fast error filtering
// on the read path. Parity with the CH `has_error` MATERIALIZED column:
//
//	0  OK / UNSET       (status_code 0 or 1, http status < 500)
//	1  HTTP 4xx         (status_code != 2 and 400 <= http < 500)
//	2  ERROR            (status_code == 2 OR http >= 500)
//
// The indexer keys error fingerprints on this bucket so the explorer's
// "errors only" facet toggle is a 1-byte PREWHERE, not a string compare.
func StatusBucket(statusCode int32, httpStatus string) uint8 {
	if statusCode == 2 {
		return 2
	}
	if httpStatus == "" {
		return 0
	}
	// Parse only the first digit — http status codes are 3-digit so the class
	// lives in [0]. Guard length to keep the parse cheap and allocation-free.
	if len(httpStatus) < 3 {
		return 0
	}
	switch httpStatus[0] {
	case '5':
		return 2
	case '4':
		return 1
	default:
		return 0
	}
}
