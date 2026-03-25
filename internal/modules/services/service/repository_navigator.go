package servicepage

import (
	"context"
	"fmt"

	"github.com/observability/observability-backend-go/internal/database"
)

func (r *ClickHouseRepository) GetServiceHealth(ctx context.Context, teamID, startMs, endMs int64) ([]serviceHealthRow, error) {
	var rows []serviceHealthRow
	err := r.db.Select(ctx, &rows, `
		SELECT service_name, request_count, error_count, error_rate, p95_latency
		FROM (
			SELECT s.service_name                                                             AS service_name,
			       toInt64(count())                                                           AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`))                                     AS error_count,
			       if(count() > 0, countIf(`+ErrorCondition()+`)*100.0/count(), 0)           AS error_rate,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95_latency
			FROM observability.spans s
			WHERE s.team_id = @teamID
			  AND `+RootSpanCondition()+`
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end
			GROUP BY s.service_name
		)
		ORDER BY request_count DESC
	`, database.SpanBaseParams(teamID, startMs, endMs)...)
	return rows, err
}
