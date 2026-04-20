package shared

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/analytics"
)

// maxAIScanRows caps the row count pulled from ClickHouse per aggregation path
// to avoid OOM on wide windows. AI spans are a small slice of total span
// traffic so this is generous in practice.
const maxAIScanRows = 50_000

type Repository interface {
	GetOverview(ctx context.Context, teamID, startMs, endMs int64) (AIOverview, error)
	GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]AITrendPoint, error)
	GetTopModels(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIModelBreakdown, error)
	GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIPromptBreakdown, error)
	GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error)
	SearchRuns(ctx context.Context, q queryContext, limit int, cursor AICursor, orderBy, orderDir string) ([]AIRunRow, bool, error)
	SummarizeRuns(ctx context.Context, q queryContext) (AIOverview, error)
	TrendRuns(ctx context.Context, q queryContext, step string) ([]AITrendPoint, error)
	FacetRuns(ctx context.Context, q queryContext) (AIExplorerFacets, error)
	RunAnalytics(ctx context.Context, teamID int64, req analytics.AnalyticsRequest, queryWhere string, queryArgs []any) (*analytics.AnalyticsResult, error)
	GetRunDetail(ctx context.Context, teamID int64, traceID, spanID string) (*AIRunDetail, error)
	GetRunLogs(ctx context.Context, teamID int64, traceID, spanID string, limit int) ([]AIRelatedLog, error)
	GetMatchingAlerts(ctx context.Context, teamID int64, detail *AIRunDetail) ([]AIAlertTargetRef, error)
}

type repository struct {
	db  clickhouse.Conn
	sql *sql.DB
}

func NewRepository(db clickhouse.Conn, sqlDB *sql.DB) Repository {
	return &repository{db: db, sql: sqlDB}
}

// rawSpanRow holds the unprocessed span fields plus the full attribute map.
// All derived AI fields (provider, model, tokens, cost, …) are computed Go-side
// from this shape; see deriveRunDetail below.
type rawSpanRow struct {
	TeamID           uint32            `ch:"team_id"`
	Timestamp        time.Time         `ch:"timestamp"`
	TraceID          string            `ch:"trace_id"`
	SpanID           string            `ch:"span_id"`
	ServiceName      string            `ch:"service_name"`
	SpanName         string            `ch:"name"`
	StatusCodeString string            `ch:"status_code_string"`
	StatusMessage    string            `ch:"status_message"`
	HasError         bool              `ch:"has_error"`
	DurationNano     int64             `ch:"duration_nano"`
	Attributes       map[string]string `ch:"attributes"`
}

// fetchRawRuns pulls raw AI span rows within the window. The WHERE clause
// is restricted to plain comparisons plus mapContains() for AI detection,
// which is not in the banned list.
func (r *repository) fetchRawRuns(ctx context.Context, q queryContext, limit int) ([]rawSpanRow, error) {
	baseWhere, args := buildAIWhereClause(q)
	if limit <= 0 {
		limit = maxAIScanRows
	}
	query := fmt.Sprintf(`
		SELECT
			s.team_id AS team_id,
			s.timestamp AS timestamp,
			s.trace_id AS trace_id,
			s.span_id AS span_id,
			s.service_name AS service_name,
			s.name AS name,
			s.status_code_string AS status_code_string,
			s.status_message AS status_message,
			s.has_error AS has_error,
			s.duration_nano AS duration_nano,
			s.attributes AS attributes
		FROM observability.spans s
		WHERE %s
		LIMIT %d
	`, baseWhere, limit)

	var rows []rawSpanRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *repository) GetOverview(ctx context.Context, teamID, startMs, endMs int64) (AIOverview, error) {
	return r.SummarizeRuns(ctx, queryContext{teamID: teamID, start: startMs, end: endMs})
}

func (r *repository) GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]AITrendPoint, error) {
	return r.TrendRuns(ctx, queryContext{teamID: teamID, start: startMs, end: endMs}, step)
}

func (r *repository) GetTopModels(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIModelBreakdown, error) {
	if limit <= 0 {
		limit = defaultBreakdownLimit
	}
	rows, err := r.fetchRawRuns(ctx, queryContext{teamID: teamID, start: startMs, end: endMs}, maxAIScanRows)
	if err != nil {
		return nil, err
	}

	type agg struct {
		provider, model                       string
		requests, errorRuns                   uint64
		latencySum, ttftSum                   float64
		latencyCount, ttftCount               int64
		qualitySum                            float64
		qualityCount                          int64
		totalTokens, costUSD                  float64
	}
	groups := make(map[string]*agg)

	for i := range rows {
		d := deriveRun(&rows[i])
		if d.RequestModel == "" {
			continue
		}
		key := d.Provider + "|" + d.RequestModel
		a := groups[key]
		if a == nil {
			a = &agg{provider: d.Provider, model: d.RequestModel}
			groups[key] = a
		}
		a.requests++
		if d.HasError {
			a.errorRuns++
		}
		a.latencySum += d.LatencyMs
		a.latencyCount++
		if d.TTFTMs > 0 {
			a.ttftSum += d.TTFTMs
			a.ttftCount++
		}
		if d.QualityScore > 0 {
			a.qualitySum += d.QualityScore
			a.qualityCount++
		}
		a.totalTokens += d.TotalTokens
		a.costUSD += d.CostUSD
	}

	out := make([]AIModelBreakdown, 0, len(groups))
	for _, a := range groups {
		item := AIModelBreakdown{
			Provider:     a.provider,
			RequestModel: a.model,
			Requests:     a.requests,
			ErrorRuns:    a.errorRuns,
			TotalTokens:  a.totalTokens,
			TotalCostUSD: a.costUSD,
		}
		if a.latencyCount > 0 {
			item.AvgLatencyMs = a.latencySum / float64(a.latencyCount)
		}
		if a.ttftCount > 0 {
			item.AvgTTFTMs = a.ttftSum / float64(a.ttftCount)
		}
		if a.qualityCount > 0 {
			item.AvgQualityScore = a.qualitySum / float64(a.qualityCount)
		}
		item.ErrorRatePct = ratioPct(item.ErrorRuns, item.Requests)
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Requests > out[j].Requests })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *repository) GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIPromptBreakdown, error) {
	if limit <= 0 {
		limit = defaultBreakdownLimit
	}
	rows, err := r.fetchRawRuns(ctx, queryContext{teamID: teamID, start: startMs, end: endMs}, maxAIScanRows)
	if err != nil {
		return nil, err
	}

	type agg struct {
		template, version      string
		requests, errorRuns    uint64
		latencySum             float64
		latencyCount           int64
		qualitySum             float64
		qualityCount           int64
		costUSD                float64
	}
	groups := make(map[string]*agg)

	for i := range rows {
		d := deriveRun(&rows[i])
		if d.PromptTemplate == "" {
			continue
		}
		key := d.PromptTemplate + "|" + d.PromptTemplateVersion
		a := groups[key]
		if a == nil {
			a = &agg{template: d.PromptTemplate, version: d.PromptTemplateVersion}
			groups[key] = a
		}
		a.requests++
		if d.HasError {
			a.errorRuns++
		}
		a.latencySum += d.LatencyMs
		a.latencyCount++
		if d.QualityScore > 0 {
			a.qualitySum += d.QualityScore
			a.qualityCount++
		}
		a.costUSD += d.CostUSD
	}

	out := make([]AIPromptBreakdown, 0, len(groups))
	for _, a := range groups {
		item := AIPromptBreakdown{
			PromptTemplate:        a.template,
			PromptTemplateVersion: a.version,
			Requests:              a.requests,
			TotalCostUSD:          a.costUSD,
		}
		if a.latencyCount > 0 {
			item.AvgLatencyMs = a.latencySum / float64(a.latencyCount)
		}
		if a.qualityCount > 0 {
			item.AvgQualityScore = a.qualitySum / float64(a.qualityCount)
		}
		item.ErrorRatePct = ratioPct(a.errorRuns, item.Requests)
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Requests > out[j].Requests })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *repository) GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error) {
	rows, err := r.fetchRawRuns(ctx, queryContext{teamID: teamID, start: startMs, end: endMs}, maxAIScanRows)
	if err != nil {
		return AIQualitySummary{}, err
	}

	var (
		qSum, fSum             float64
		qCount, fCount         int64
		guardrailBlocks        uint64
		guardrailCounts        = make(map[string]uint64)
		qualityBucketCounts    = make(map[string]uint64)
	)
	for i := range rows {
		d := deriveRun(&rows[i])
		if d.QualityScore > 0 {
			qSum += d.QualityScore
			qCount++
		}
		if d.FeedbackScore > 0 {
			fSum += d.FeedbackScore
			fCount++
		}
		guardrailKey := d.GuardrailState
		if guardrailKey == "" {
			guardrailKey = "not_evaluated"
		}
		guardrailCounts[guardrailKey]++
		if isGuardrailBlocked(d.GuardrailState) {
			guardrailBlocks++
		}
		qualityBucketCounts[d.QualityBucket]++
	}

	summary := AIQualitySummary{
		ScoredRuns:      uint64(qCount),
		FeedbackRuns:    uint64(fCount),
		GuardrailBlocks: guardrailBlocks,
	}
	if qCount > 0 {
		summary.AvgQualityScore = qSum / float64(qCount)
	}
	if fCount > 0 {
		summary.AvgFeedbackScore = fSum / float64(fCount)
	}

	summary.GuardrailBreakdown = make([]AIGuardrailBreakdown, 0, len(guardrailCounts))
	for k, v := range guardrailCounts {
		summary.GuardrailBreakdown = append(summary.GuardrailBreakdown, AIGuardrailBreakdown{GuardrailState: k, Count: v})
	}
	sort.SliceStable(summary.GuardrailBreakdown, func(i, j int) bool {
		return summary.GuardrailBreakdown[i].Count > summary.GuardrailBreakdown[j].Count
	})

	summary.QualityBuckets = make([]AIQualityBucket, 0, len(qualityBucketCounts))
	for k, v := range qualityBucketCounts {
		summary.QualityBuckets = append(summary.QualityBuckets, AIQualityBucket{Bucket: k, Count: v})
	}
	sort.SliceStable(summary.QualityBuckets, func(i, j int) bool {
		return summary.QualityBuckets[i].Count > summary.QualityBuckets[j].Count
	})

	return summary, nil
}

func (r *repository) SearchRuns(ctx context.Context, q queryContext, limit int, cursor AICursor, orderBy, orderDir string) ([]AIRunRow, bool, error) {
	if limit <= 0 || limit > maxPageSize {
		limit = defaultPageSize
	}

	// Fetch raw spans ordered by timestamp and filtered Go-side on cursor; we
	// over-fetch to allow Go-side sort on non-timestamp orderBys and pagination.
	fetchLimit := limit*10 + 1
	if fetchLimit < 200 {
		fetchLimit = 200
	}
	if fetchLimit > maxAIScanRows {
		fetchLimit = maxAIScanRows
	}

	rows, err := r.fetchRawRuns(ctx, q, fetchLimit)
	if err != nil {
		return nil, false, err
	}

	derived := make([]AIRunRow, 0, len(rows))
	for i := range rows {
		d := deriveRun(&rows[i])
		derived = append(derived, d.AIRunRow)
	}

	asc := strings.EqualFold(strings.TrimSpace(orderDir), "asc")
	key := strings.ToLower(strings.TrimSpace(orderBy))
	sort.SliceStable(derived, func(i, j int) bool {
		return runLess(derived[i], derived[j], key, asc)
	})

	// Apply cursor after sort.
	if !cursor.StartTime.IsZero() {
		filtered := derived[:0]
		for _, row := range derived {
			if runPastCursor(row, cursor, key, asc) {
				filtered = append(filtered, row)
			}
		}
		derived = filtered
	}

	hasMore := len(derived) > limit
	if hasMore {
		derived = derived[:limit]
	}
	return derived, hasMore, nil
}

func (r *repository) SummarizeRuns(ctx context.Context, q queryContext) (AIOverview, error) {
	rows, err := r.fetchRawRuns(ctx, q, maxAIScanRows)
	if err != nil {
		return AIOverview{}, err
	}

	var (
		totalRuns, errorRuns, guardrailBlocked          uint64
		latencySum, ttftSum                             float64
		latencyCount, ttftCount                         int64
		totalTokens, totalCost                          float64
		qSum                                            float64
		qCount                                          int64
		providers, models, prompts, services            = make(map[string]struct{}), make(map[string]struct{}), make(map[string]struct{}), make(map[string]struct{})
	)
	for i := range rows {
		d := deriveRun(&rows[i])
		totalRuns++
		if d.HasError {
			errorRuns++
		}
		latencySum += d.LatencyMs
		latencyCount++
		if d.TTFTMs > 0 {
			ttftSum += d.TTFTMs
			ttftCount++
		}
		totalTokens += d.TotalTokens
		totalCost += d.CostUSD
		if d.QualityScore > 0 {
			qSum += d.QualityScore
			qCount++
		}
		if isGuardrailBlocked(d.GuardrailState) {
			guardrailBlocked++
		}
		if d.Provider != "" {
			providers[d.Provider] = struct{}{}
		}
		if d.RequestModel != "" {
			models[d.RequestModel] = struct{}{}
		}
		if d.PromptTemplate != "" {
			prompts[d.PromptTemplate] = struct{}{}
		}
		if d.ServiceName != "" {
			services[d.ServiceName] = struct{}{}
		}
	}

	summary := AIOverview{
		TotalRuns:        totalRuns,
		ErrorRuns:        errorRuns,
		TotalTokens:      totalTokens,
		TotalCostUSD:     totalCost,
		ProviderCount:    uint64(len(providers)),
		ModelCount:       uint64(len(models)),
		PromptCount:      uint64(len(prompts)),
		GuardrailBlocked: guardrailBlocked,
	}
	if latencyCount > 0 {
		summary.AvgLatencyMs = latencySum / float64(latencyCount)
	}
	if ttftCount > 0 {
		summary.AvgTTFTMs = ttftSum / float64(ttftCount)
	}
	if qCount > 0 {
		summary.AvgQualityScore = qSum / float64(qCount)
	}
	summary.ErrorRatePct = ratioPct(summary.ErrorRuns, summary.TotalRuns)

	summary.Services = make([]string, 0, len(services))
	for svc := range services {
		summary.Services = append(summary.Services, svc)
	}
	sort.Strings(summary.Services)
	return summary, nil
}

func (r *repository) TrendRuns(ctx context.Context, q queryContext, step string) ([]AITrendPoint, error) {
	rows, err := r.fetchRawRuns(ctx, q, maxAIScanRows)
	if err != nil {
		return nil, err
	}

	stepSec := stepToSeconds(step)
	buckets := make(map[int64]*AITrendPoint)
	type accum struct {
		ttftSum                 float64
		ttftCount               int64
		qSum                    float64
		qCount                  int64
	}
	accums := make(map[int64]*accum)

	for i := range rows {
		d := deriveRun(&rows[i])
		ts := bucketStart(d.StartTime, stepSec)
		pt := buckets[ts]
		if pt == nil {
			pt = &AITrendPoint{TimeBucket: time.Unix(ts, 0).UTC()}
			buckets[ts] = pt
			accums[ts] = &accum{}
		}
		pt.Requests++
		if d.HasError {
			pt.ErrorRuns++
		}
		pt.TotalTokens += d.TotalTokens
		pt.TotalCostUSD += d.CostUSD
		if isGuardrailBlocked(d.GuardrailState) {
			pt.GuardrailBlocked++
		}
		a := accums[ts]
		if d.TTFTMs > 0 {
			a.ttftSum += d.TTFTMs
			a.ttftCount++
		}
		if d.QualityScore > 0 {
			a.qSum += d.QualityScore
			a.qCount++
		}
	}

	tsKeys := make([]int64, 0, len(buckets))
	for ts := range buckets {
		tsKeys = append(tsKeys, ts)
	}
	sort.Slice(tsKeys, func(i, j int) bool { return tsKeys[i] < tsKeys[j] })

	out := make([]AITrendPoint, 0, len(tsKeys))
	for _, ts := range tsKeys {
		pt := buckets[ts]
		a := accums[ts]
		if a.ttftCount > 0 {
			pt.AvgTTFTMs = a.ttftSum / float64(a.ttftCount)
		}
		if a.qCount > 0 {
			pt.AvgQualityScore = a.qSum / float64(a.qCount)
		}
		pt.ErrorRatePct = ratioPct(pt.ErrorRuns, pt.Requests)
		out = append(out, *pt)
	}
	return out, nil
}

func (r *repository) FacetRuns(ctx context.Context, q queryContext) (AIExplorerFacets, error) {
	rows, err := r.fetchRawRuns(ctx, q, maxAIScanRows)
	if err != nil {
		return AIExplorerFacets{}, err
	}

	counts := map[string]map[string]uint64{
		"provider":        {},
		"request_model":   {},
		"operation":       {},
		"service_name":    {},
		"prompt_template": {},
		"finish_reason":   {},
		"guardrail_state": {},
		"quality_bucket":  {},
		"status":          {},
	}
	for i := range rows {
		d := deriveRun(&rows[i])
		if d.Provider != "" {
			counts["provider"][d.Provider]++
		}
		if d.RequestModel != "" {
			counts["request_model"][d.RequestModel]++
		}
		if d.Operation != "" {
			counts["operation"][d.Operation]++
		}
		if d.ServiceName != "" {
			counts["service_name"][d.ServiceName]++
		}
		if d.PromptTemplate != "" {
			counts["prompt_template"][d.PromptTemplate]++
		}
		if d.FinishReason != "" {
			counts["finish_reason"][d.FinishReason]++
		}
		grd := d.GuardrailState
		if grd == "" {
			grd = "not_evaluated"
		}
		counts["guardrail_state"][grd]++
		if d.QualityBucket != "" {
			counts["quality_bucket"][d.QualityBucket]++
		}
		if d.Status != "" {
			counts["status"][d.Status]++
		}
	}

	var out AIExplorerFacets
	out.Provider = topBuckets(counts["provider"], defaultBreakdownLimit)
	out.RequestModel = topBuckets(counts["request_model"], defaultBreakdownLimit)
	out.Operation = topBuckets(counts["operation"], defaultBreakdownLimit)
	out.ServiceName = topBuckets(counts["service_name"], defaultBreakdownLimit)
	out.PromptTemplate = topBuckets(counts["prompt_template"], defaultBreakdownLimit)
	out.FinishReason = topBuckets(counts["finish_reason"], defaultBreakdownLimit)
	out.GuardrailState = topBuckets(counts["guardrail_state"], defaultBreakdownLimit)
	out.QualityBucket = topBuckets(counts["quality_bucket"], defaultBreakdownLimit)
	out.Status = topBuckets(counts["status"], defaultBreakdownLimit)
	return out, nil
}

// RunAnalytics runs the cross-dimensional analytics query entirely Go-side.
// Rows are fetched via the plain span projection; dimensions and aggregations
// are resolved against the derived AIRunDetail shape. Percentiles fall back
// to sorted-sample nearest-rank (no CH-side quantileTDigest).
func (r *repository) RunAnalytics(ctx context.Context, teamID int64, req analytics.AnalyticsRequest, queryWhere string, queryArgs []any) (*analytics.AnalyticsResult, error) {
	q := queryContext{teamID: teamID, start: req.StartTime, end: req.EndTime, where: queryWhere, args: queryArgs}
	rows, err := r.fetchRawRuns(ctx, q, maxAIScanRows)
	if err != nil {
		return nil, err
	}

	derived := make([]AIRunDetail, 0, len(rows))
	for i := range rows {
		derived = append(derived, deriveRun(&rows[i]))
	}

	return computeAnalyticsResult(req, derived), nil
}

func (r *repository) GetRunDetail(ctx context.Context, teamID int64, traceID, spanID string) (*AIRunDetail, error) {
	query := `
		SELECT
			s.team_id AS team_id,
			s.timestamp AS timestamp,
			s.trace_id AS trace_id,
			s.span_id AS span_id,
			s.service_name AS service_name,
			s.name AS name,
			s.status_code_string AS status_code_string,
			s.status_message AS status_message,
			s.has_error AS has_error,
			s.duration_nano AS duration_nano,
			s.attributes AS attributes
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID AND s.span_id = @spanID
		LIMIT 1
	`

	var rows []rawSpanRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
		clickhouse.Named("spanID", spanID),
	); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	detail := deriveRun(&rows[0])
	return &detail, nil
}

func (r *repository) GetRunLogs(ctx context.Context, teamID int64, traceID, spanID string, limit int) ([]AIRelatedLog, error) {
	if limit <= 0 || limit > 200 {
		limit = defaultLogLimit
	}

	query := `
		SELECT
			timestamp,
			severity_text,
			body,
			service AS service_name,
			trace_id,
			span_id
		FROM observability.logs
		WHERE team_id = @teamID
		  AND trace_id = @traceID
		  AND (span_id = @spanID OR @spanID = '')
		ORDER BY timestamp DESC
		LIMIT @limit
	`

	var rows []logRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
		clickhouse.Named("spanID", spanID),
		clickhouse.Named("limit", limit),
	); err != nil {
		return nil, err
	}

	out := make([]AIRelatedLog, 0, len(rows))
	for _, row := range rows {
		out = append(out, AIRelatedLog{
			Timestamp:    row.Timestamp,
			SeverityText: row.SeverityText,
			Body:         row.Body,
			ServiceName:  row.ServiceName,
			TraceID:      row.TraceID,
			SpanID:       row.SpanID,
		})
	}
	return out, nil
}

func (r *repository) GetMatchingAlerts(ctx context.Context, teamID int64, detail *AIRunDetail) ([]AIAlertTargetRef, error) {
	if r.sql == nil || detail == nil {
		return nil, nil
	}

	rows, err := r.sql.QueryContext(ctx, `
		SELECT id, name, condition_type, rule_state, target_ref
		FROM observability.alerts
		WHERE team_id = ? AND enabled = 1
		ORDER BY id DESC
		LIMIT 200
	`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []alertRow
	for rows.Next() {
		var row alertRow
		if scanErr := rows.Scan(&row.ID, &row.Name, &row.ConditionType, &row.RuleState, &row.TargetRef); scanErr != nil {
			return nil, scanErr
		}
		candidates = append(candidates, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]AIAlertTargetRef, 0, len(candidates))
	for _, candidate := range candidates {
		var target map[string]any
		if err := json.Unmarshal([]byte(candidate.TargetRef), &target); err != nil {
			continue
		}
		if !targetMatchesRun(target, detail) {
			continue
		}
		out = append(out, AIAlertTargetRef{
			AlertID:       candidate.ID,
			RuleName:      candidate.Name,
			ConditionType: candidate.ConditionType,
			RuleState:     candidate.RuleState,
			TargetRef:     target,
		})
	}
	return out, nil
}

// buildAIWhereClause returns a WHERE fragment using only plain comparisons.
// AI-adjacent spans are isolated via mapContains() over the gen_ai/llm/optikk
// attribute keys (mapContains is not a banned combinator).
func buildAIWhereClause(q queryContext) (string, []any) {
	where := "s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end AND " + aiDetectionExpr("s")
	args := []any{
		clickhouse.Named("teamID", uint32(q.teamID)),
		clickhouse.Named("bucketStart", utils.SpansBucketStart(q.start/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(q.end/1000)),
		clickhouse.Named("start", time.UnixMilli(q.start)),
		clickhouse.Named("end", time.UnixMilli(q.end)),
	}
	if strings.TrimSpace(q.where) != "" {
		where += " AND " + q.where
		args = append(args, q.args...)
	}
	return where, args
}

func ratioPct(errorRuns, totalRuns uint64) float64 {
	if totalRuns == 0 {
		return 0
	}
	return (float64(errorRuns) / float64(totalRuns)) * 100
}

// aiDetectionExpr keeps the filter inside the WHERE clause using only the
// permitted mapContains() calls — no conditionals, no casts, no combinators.
func aiDetectionExpr(alias string) string {
	parts := make([]string, 0, len(aiDetectionAttributeKeys))
	for _, key := range aiDetectionAttributeKeys {
		parts = append(parts, fmt.Sprintf("mapContains(%s.attributes, '%s')", alias, key))
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// deriveRun is the single source of truth for attribute-derived AI fields.
// Every aggregation path runs through this so CH never has to compute
// multiIf/mapGet fallbacks inside the SELECT clause.
func deriveRun(row *rawSpanRow) AIRunDetail {
	provider := firstNonEmptyAttr(row.Attributes, providerAttributeKeys)
	requestModel := firstNonEmptyAttr(row.Attributes, requestModelAttributeKeys)
	responseModel := firstNonEmptyAttr(row.Attributes, responseModelAttributeKeys)
	if responseModel == "" {
		responseModel = requestModel
	}
	operation := firstNonEmptyAttr(row.Attributes, operationAttributeKeys)
	if operation == "" {
		operation = row.SpanName
	}

	inputTokens := firstNumericAttr(row.Attributes, inputTokensAttributeKeys)
	outputTokens := firstNumericAttr(row.Attributes, outputTokensAttributeKeys)
	totalTokens := firstNumericAttr(row.Attributes, totalTokensAttributeKeys)
	if !anyAttrPresent(row.Attributes, totalTokensAttributeKeys) {
		sum := inputTokens + outputTokens
		if sum > 0 {
			totalTokens = sum
		} else {
			totalTokens = 0
		}
	}

	qualityScore := firstNumericAttr(row.Attributes, qualityScoreAttributeKeys)

	status := row.StatusCodeString
	if status == "" {
		if row.HasError {
			status = "ERROR"
		} else {
			status = "UNSET"
		}
	}

	detail := AIRunDetail{
		AIRunRow: AIRunRow{
			RunID:                 row.TraceID + ":" + row.SpanID,
			TraceID:               row.TraceID,
			SpanID:                row.SpanID,
			ServiceName:           row.ServiceName,
			Provider:              provider,
			RequestModel:          requestModel,
			ResponseModel:         responseModel,
			Operation:             operation,
			PromptTemplate:        firstNonEmptyAttr(row.Attributes, promptTemplateAttributeKeys),
			PromptTemplateVersion: firstNonEmptyAttr(row.Attributes, promptTemplateVersionAttributeKeys),
			ConversationID:        firstNonEmptyAttr(row.Attributes, conversationAttributeKeys),
			SessionID:             firstNonEmptyAttr(row.Attributes, sessionAttributeKeys),
			FinishReason:          firstNonEmptyAttr(row.Attributes, finishReasonAttributeKeys),
			GuardrailState:        firstNonEmptyAttr(row.Attributes, guardrailAttributeKeys),
			Status:                status,
			StatusMessage:         row.StatusMessage,
			HasError:              row.HasError,
			StartTime:             row.Timestamp,
			LatencyMs:             float64(row.DurationNano) / 1_000_000.0,
			TTFTMs:                firstNumericAttr(row.Attributes, ttftAttributeKeys),
			InputTokens:           inputTokens,
			OutputTokens:          outputTokens,
			TotalTokens:           totalTokens,
			CostUSD:               firstNumericAttr(row.Attributes, costAttributeKeys),
			QualityScore:          qualityScore,
			FeedbackScore:         firstNumericAttr(row.Attributes, feedbackScoreAttributeKeys),
			QualityBucket:         qualityBucketFor(qualityScore),
		},
		SpanName:        row.SpanName,
		ServiceVersion:  firstNonEmptyAttr(row.Attributes, serviceVersionAttributeKeys),
		Environment:     firstNonEmptyAttr(row.Attributes, environmentAttributeKeys),
		PromptSnippet:   firstNonEmptyAttr(row.Attributes, promptSnippetAttributeKeys),
		ResponseSnippet: firstNonEmptyAttr(row.Attributes, responseSnippetAttributeKeys),
		CacheHit:        firstBoolAttr(row.Attributes, cacheHitAttributeKeys),
		RawAttributes:   row.Attributes,
	}
	return detail
}

func firstNonEmptyAttr(attrs map[string]string, keys []string) string {
	for _, k := range keys {
		if v, ok := attrs[k]; ok && v != "" {
			return v
		}
	}
	return ""
}

func firstNumericAttr(attrs map[string]string, keys []string) float64 {
	for _, k := range keys {
		v, ok := attrs[k]
		if !ok || v == "" {
			continue
		}
		if f := utils.Float64FromAny(v); !math.IsNaN(f) {
			return f
		}
	}
	return 0
}

func anyAttrPresent(attrs map[string]string, keys []string) bool {
	for _, k := range keys {
		if _, ok := attrs[k]; ok {
			return true
		}
	}
	return false
}

func firstBoolAttr(attrs map[string]string, keys []string) bool {
	for _, k := range keys {
		v, ok := attrs[k]
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes":
			return true
		case "0", "false", "no":
			return false
		}
	}
	return false
}

func qualityBucketFor(score float64) string {
	switch {
	case score >= 0.9:
		return "excellent"
	case score >= 0.75:
		return "good"
	case score > 0:
		return "review"
	default:
		return "unscored"
	}
}

func isGuardrailBlocked(state string) bool {
	switch strings.ToLower(state) {
	case "blocked", "rejected", "failed":
		return true
	}
	return false
}

func topBuckets(counts map[string]uint64, limit int) []FacetBucket {
	out := make([]FacetBucket, 0, len(counts))
	for k, v := range counts {
		out = append(out, FacetBucket{Value: k, Count: v})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Value < out[j].Value
		}
		return out[i].Count > out[j].Count
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func stepToSeconds(step string) int64 {
	switch strings.ToLower(strings.TrimSpace(step)) {
	case "1m", "minute":
		return 60
	case "5m", "5minute":
		return 300
	case "15m", "15minute":
		return 900
	case "1h", "hour":
		return 3600
	case "1d", "day":
		return 86400
	default:
		return 300
	}
}

func bucketStart(ts time.Time, stepSec int64) int64 {
	if stepSec <= 0 {
		stepSec = 300
	}
	return (ts.Unix() / stepSec) * stepSec
}

func runLess(a, b AIRunRow, key string, asc bool) bool {
	var less bool
	switch key {
	case "latency_ms":
		less = a.LatencyMs < b.LatencyMs
	case "ttft_ms":
		less = a.TTFTMs < b.TTFTMs
	case "cost_usd":
		less = a.CostUSD < b.CostUSD
	case "quality_score":
		less = a.QualityScore < b.QualityScore
	case "total_tokens":
		less = a.TotalTokens < b.TotalTokens
	case "start_time", "":
		less = a.StartTime.Before(b.StartTime)
	default:
		less = a.StartTime.Before(b.StartTime)
	}
	if asc {
		return less
	}
	return !less
}

func runPastCursor(row AIRunRow, cur AICursor, key string, asc bool) bool {
	// Cursor for non-timestamp orderBys is best-effort; we still gate by
	// the serialized (StartTime, SpanID) tuple.
	cmpTime := row.StartTime.Compare(cur.StartTime)
	if cmpTime == 0 {
		if asc {
			return row.SpanID > cur.SpanID
		}
		return row.SpanID < cur.SpanID
	}
	if asc {
		return cmpTime > 0
	}
	return cmpTime < 0
}

func targetMatchesRun(target map[string]any, detail *AIRunDetail) bool {
	matchers := 0
	matched := 0

	compare := func(value string, keys ...string) {
		if value == "" {
			return
		}
		for _, key := range keys {
			raw, ok := target[key]
			if !ok {
				continue
			}
			str, ok := raw.(string)
			if !ok || str == "" {
				continue
			}
			matchers++
			if strings.EqualFold(str, value) {
				matched++
			}
			return
		}
	}

	compare(detail.ServiceName, "service_name", "service", "serviceName")
	compare(detail.Provider, "provider")
	compare(detail.RequestModel, "model", "request_model", "requestModel")
	compare(detail.PromptTemplate, "prompt_template", "promptTemplate")
	compare(detail.GuardrailState, "guardrail_state", "guardrailState")

	if matchers == 0 {
		return false
	}
	return matched == matchers
}
