package tracecompare

import (
	"fmt"
	"sort"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// Compare fetches both traces and computes structural + latency diffs.
func (s *Service) Compare(teamID int64, traceIDA, traceIDB string) (*ComparisonResult, error) {
	spansA, err := s.repo.FetchTraceSpans(teamID, traceIDA)
	if err != nil {
		return nil, fmt.Errorf("fetching trace A: %w", err)
	}
	spansB, err := s.repo.FetchTraceSpans(teamID, traceIDB)
	if err != nil {
		return nil, fmt.Errorf("fetching trace B: %w", err)
	}
	if len(spansA) == 0 {
		return nil, fmt.Errorf("trace %s not found", traceIDA)
	}
	if len(spansB) == 0 {
		return nil, fmt.Errorf("trace %s not found", traceIDB)
	}

	assignDepths(spansA)
	assignDepths(spansB)

	sigA := buildSignatureMap(spansA)
	sigB := buildSignatureMap(spansB)

	var matched []SpanPairDiff
	var onlyInA []SpanSummary
	matchedKeysB := make(map[string]bool)

	for key, a := range sigA {
		if b, ok := sigB[key]; ok {
			deltaMs := b.DurationMs - a.DurationMs
			deltaPct := 0.0
			if a.DurationMs > 0 {
				deltaPct = deltaMs * 100.0 / a.DurationMs
			}
			matched = append(matched, SpanPairDiff{
				Signature:     a.sig,
				SpanIDA:       a.SpanID,
				SpanIDB:       b.SpanID,
				DurationMsA:   a.DurationMs,
				DurationMsB:   b.DurationMs,
				DeltaMs:       deltaMs,
				DeltaPct:      deltaPct,
				StatusA:       a.Status,
				StatusB:       b.Status,
				StatusChanged: a.Status != b.Status,
			})
			matchedKeysB[key] = true
		} else {
			onlyInA = append(onlyInA, spanToSummary(a))
		}
	}

	var onlyInB []SpanSummary
	for key, b := range sigB {
		if !matchedKeysB[key] {
			onlyInB = append(onlyInB, spanToSummary(b))
		}
	}

	// Sort matched by absolute delta descending (biggest changes first).
	sort.Slice(matched, func(i, j int) bool {
		ai := matched[i].DeltaMs
		aj := matched[j].DeltaMs
		if ai < 0 {
			ai = -ai
		}
		if aj < 0 {
			aj = -aj
		}
		return ai > aj
	})

	summaryA := buildTraceSummary(traceIDA, spansA)
	summaryB := buildTraceSummary(traceIDB, spansB)

	return &ComparisonResult{
		TraceA:        summaryA,
		TraceB:        summaryB,
		MatchedSpans:  matched,
		OnlyInA:       onlyInA,
		OnlyInB:       onlyInB,
		ServiceDeltas: computeServiceDeltas(spansA, spansB),
		TotalDeltaMs:  summaryB.DurationMs - summaryA.DurationMs,
	}, nil
}

// assignDepths computes the depth of each span via parent-child traversal.
func assignDepths(spans []internalSpan) {
	idx := make(map[string]int, len(spans))
	for i := range spans {
		idx[spans[i].SpanID] = i
	}
	for i := range spans {
		depth := 0
		cur := spans[i].ParentID
		for cur != "" {
			depth++
			if pi, ok := idx[cur]; ok {
				cur = spans[pi].ParentID
			} else {
				break
			}
			if depth > 100 {
				break
			}
		}
		spans[i].Depth = depth
	}
}

type sigEntry struct {
	internalSpan
	sig SpanSignature
}

// buildSignatureMap creates a lookup from canonical signature to span.
// For duplicate signatures, keeps the first occurrence.
func buildSignatureMap(spans []internalSpan) map[string]sigEntry {
	m := make(map[string]sigEntry, len(spans))
	for _, s := range spans {
		sig := SpanSignature{
			Service:   s.Service,
			Operation: s.Operation,
			SpanKind:  s.SpanKind,
			Depth:     s.Depth,
		}
		key := fmt.Sprintf("%s|%s|%s|%d", sig.Service, sig.Operation, sig.SpanKind, sig.Depth)
		if _, exists := m[key]; !exists {
			m[key] = sigEntry{internalSpan: s, sig: sig}
		}
	}
	return m
}

func spanToSummary(e sigEntry) SpanSummary {
	return SpanSummary{
		SpanID:     e.SpanID,
		Service:    e.Service,
		Operation:  e.Operation,
		SpanKind:   e.SpanKind,
		DurationMs: e.DurationMs,
		Status:     e.Status,
	}
}

func buildTraceSummary(traceID string, spans []internalSpan) TraceSummary {
	services := make(map[string]bool)
	var maxDuration float64
	var errorCount int
	for _, s := range spans {
		services[s.Service] = true
		if s.Depth == 0 && s.DurationMs > maxDuration {
			maxDuration = s.DurationMs
		}
		if s.HasError {
			errorCount++
		}
	}
	return TraceSummary{
		TraceID:    traceID,
		SpanCount:  len(spans),
		DurationMs: maxDuration,
		ErrorCount: errorCount,
		Services:   len(services),
	}
}

func computeServiceDeltas(spansA, spansB []internalSpan) []ServiceDelta {
	type svcStats struct {
		totalMs   float64
		spanCount int
	}
	statsA := make(map[string]*svcStats)
	for _, s := range spansA {
		st, ok := statsA[s.Service]
		if !ok {
			st = &svcStats{}
			statsA[s.Service] = st
		}
		st.totalMs += s.DurationMs
		st.spanCount++
	}
	statsB := make(map[string]*svcStats)
	for _, s := range spansB {
		st, ok := statsB[s.Service]
		if !ok {
			st = &svcStats{}
			statsB[s.Service] = st
		}
		st.totalMs += s.DurationMs
		st.spanCount++
	}

	allServices := make(map[string]bool)
	for k := range statsA {
		allServices[k] = true
	}
	for k := range statsB {
		allServices[k] = true
	}

	deltas := make([]ServiceDelta, 0, len(allServices))
	for svc := range allServices {
		a := statsA[svc]
		b := statsB[svc]
		d := ServiceDelta{Service: svc}
		if a != nil {
			d.TotalMsA = a.totalMs
			d.SpanCountA = a.spanCount
		}
		if b != nil {
			d.TotalMsB = b.totalMs
			d.SpanCountB = b.spanCount
		}
		d.DeltaMs = d.TotalMsB - d.TotalMsA
		deltas = append(deltas, d)
	}

	sort.Slice(deltas, func(i, j int) bool {
		di := deltas[i].DeltaMs
		dj := deltas[j].DeltaMs
		if di < 0 {
			di = -di
		}
		if dj < 0 {
			dj = -dj
		}
		return di > dj
	})
	return deltas
}
