package querybuilder

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// BasePrewhere returns the standard PREWHERE clause and parameters for the given team and time window.
// It uses ts_bucket_start for pruning.
func BasePrewhere(teamID int64, startMs, endMs int64) (string, []any) {
	clause := "PREWHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd"
	
	// Assuming bucket sizes are in hours or similar based on existing utils
	bucketStart := startMs / 1000 
	bucketEnd := endMs / 1000

	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", uint64(bucketStart)),
		clickhouse.Named("bucketEnd", uint64(bucketEnd)),
	}
	return clause, args
}

// FingerprintPrewhere adds the resource_fingerprint resolution to the PREWHERE clause.
// This implements the SigNoz pattern: resolve fingerprints first, then scan main table.
func FingerprintPrewhere(teamID int64, startMs, endMs int64, fingerprint string) (string, []any) {
	baseClause, baseArgs := BasePrewhere(teamID, startMs, endMs)
	
	if fingerprint != "" {
		clause := fmt.Sprintf("%s AND resource_fingerprint = @fingerprint", baseClause)
		args := append(baseArgs, clickhouse.Named("fingerprint", fingerprint))
		return clause, args
	}
	
	return baseClause, baseArgs
}
