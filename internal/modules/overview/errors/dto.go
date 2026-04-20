package errors

// Mapper functions that convert CH scan rows (uint64 counts / raw nanos)
// into the int64 / derived-ratio public shapes. All casts + ratio math
// happens here so repository SQL stays strictly column-raw.

func mapErrorGroupRows(rows []errorGroupRow) []ErrorGroup {
	groups := make([]ErrorGroup, len(rows))
	for i, row := range rows {
		code := int(row.HTTPStatusCode)
		groups[i] = ErrorGroup{
			GroupID:         ErrorGroupID(row.ServiceName, row.OperationName, row.StatusMessage, code),
			ServiceName:     row.ServiceName,
			OperationName:   row.OperationName,
			StatusMessage:   row.StatusMessage,
			HTTPStatusCode:  code,
			ErrorCount:      int64(row.ErrorCount), //nolint:gosec // count() is domain-bounded
			LastOccurrence:  row.LastOccurrence,
			FirstOccurrence: row.FirstOccurrence,
			SampleTraceID:   row.SampleTraceID,
		}
	}
	return groups
}

func mapErrorGroupDetailRow(groupID string, row *errorGroupDetailRow) *ErrorGroupDetail {
	if row == nil {
		return nil
	}
	return &ErrorGroupDetail{
		GroupID:         groupID,
		ServiceName:     row.ServiceName,
		OperationName:   row.OperationName,
		StatusMessage:   row.StatusMessage,
		HTTPStatusCode:  int(row.HTTPStatusCode),
		ErrorCount:      int64(row.ErrorCount), //nolint:gosec // count() is domain-bounded
		LastOccurrence:  row.LastOccurrence,
		FirstOccurrence: row.FirstOccurrence,
		SampleTraceID:   row.SampleTraceID,
		ExceptionType:   row.ExceptionType,
		StackTrace:      row.StackTrace,
	}
}

func mapErrorGroupTraceRows(rows []errorGroupTraceRow) []ErrorGroupTrace {
	traces := make([]ErrorGroupTrace, len(rows))
	for i, row := range rows {
		traces[i] = ErrorGroupTrace{
			TraceID:    row.TraceID,
			SpanID:     row.SpanID,
			Timestamp:  row.Timestamp,
			DurationMs: float64(row.DurationNanos) / 1_000_000.0,
			StatusCode: row.StatusCode,
		}
	}
	return traces
}

func mapErrorGroupTimeseriesRows(rows []errorGroupTSRow) []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			Timestamp:  row.Timestamp,
			ErrorCount: int64(row.ErrorCount), //nolint:gosec // count() is domain-bounded
		}
	}
	return points
}

func mapServiceErrorRateRows(rows []serviceErrorRateRow) []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		reqCount := int64(row.RequestCount)   //nolint:gosec // count() is domain-bounded
		errCount := int64(row.ErrorCount)     //nolint:gosec // count() is domain-bounded
		durSum := float64(row.DurationNanosSum) / 1_000_000.0
		var errRate, avgLatency float64
		if row.RequestCount > 0 {
			errRate = float64(row.ErrorCount) * 100.0 / float64(row.RequestCount)
			avgLatency = durSum / float64(row.RequestCount)
		}
		points[i] = TimeSeriesPoint{
			ServiceName:  row.ServiceName,
			Timestamp:    row.Timestamp,
			RequestCount: reqCount,
			ErrorCount:   errCount,
			ErrorRate:    errRate,
			AvgLatency:   avgLatency,
		}
	}
	return points
}

func mapErrorVolumeRows(rows []errorVolumeRow) []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, 0, len(rows))
	for _, row := range rows {
		if row.ErrorCount == 0 {
			continue
		}
		points = append(points, TimeSeriesPoint{
			ServiceName: row.ServiceName,
			Timestamp:   row.Timestamp,
			ErrorCount:  int64(row.ErrorCount), //nolint:gosec // count() is domain-bounded
		})
	}
	return points
}

func mapLatencyErrorRows(rows []latencyErrorRow) []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, 0, len(rows))
	for _, row := range rows {
		if row.ErrorCount == 0 {
			continue
		}
		var avgLatency float64
		if row.RequestCount > 0 {
			avgLatency = float64(row.DurationNanosSum) / 1_000_000.0 / float64(row.RequestCount)
		}
		points = append(points, TimeSeriesPoint{
			ServiceName:  row.ServiceName,
			Timestamp:    row.Timestamp,
			RequestCount: int64(row.RequestCount), //nolint:gosec // count() is domain-bounded
			ErrorCount:   int64(row.ErrorCount),   //nolint:gosec // count() is domain-bounded
			AvgLatency:   avgLatency,
		})
	}
	return points
}

func mapExceptionRateRows(rows []exceptionRateRawRow) []ExceptionRatePoint {
	out := make([]ExceptionRatePoint, len(rows))
	for i, row := range rows {
		out[i] = ExceptionRatePoint{
			Timestamp:     row.Timestamp,
			ExceptionType: row.ExceptionType,
			Count:         int64(row.Count), //nolint:gosec // count() is domain-bounded
		}
	}
	return out
}

func mapErrorHotspotRows(rows []errorHotspotRawRow) []ErrorHotspotCell {
	out := make([]ErrorHotspotCell, len(rows))
	for i, row := range rows {
		var rate float64
		if row.TotalCount > 0 {
			rate = float64(row.ErrorCount) * 100.0 / float64(row.TotalCount)
		}
		out[i] = ErrorHotspotCell{
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			ErrorRate:     rate,
			ErrorCount:    int64(row.ErrorCount), //nolint:gosec // count() is domain-bounded
			TotalCount:    int64(row.TotalCount), //nolint:gosec // count() is domain-bounded
		}
	}
	return out
}

func mapHTTP5xxByRouteRows(rows []http5xxByRouteRawRow) []HTTP5xxByRoute {
	out := make([]HTTP5xxByRoute, len(rows))
	for i, row := range rows {
		out[i] = HTTP5xxByRoute{
			HTTPRoute:   row.HTTPRoute,
			ServiceName: row.ServiceName,
			Count:       int64(row.Count), //nolint:gosec // count() is domain-bounded
		}
	}
	return out
}

func mapFingerprintRows(rows []errorFingerprintRawRow) []ErrorFingerprint {
	out := make([]ErrorFingerprint, len(rows))
	for i, row := range rows {
		out[i] = ErrorFingerprint{
			Fingerprint:   row.Fingerprint,
			ServiceName:   row.ServiceName,
			OperationName: row.OperationName,
			ExceptionType: row.ExceptionType,
			StatusMessage: row.StatusMessage,
			FirstSeen:     row.FirstSeen,
			LastSeen:      row.LastSeen,
			Count:         int64(row.Count), //nolint:gosec // count() is domain-bounded
			SampleTraceID: row.SampleTraceID,
		}
	}
	return out
}

func mapFingerprintTrendRows(rows []fingerprintTrendRawRow) []FingerprintTrendPoint {
	out := make([]FingerprintTrendPoint, len(rows))
	for i, row := range rows {
		out[i] = FingerprintTrendPoint{
			Timestamp: row.Timestamp,
			Count:     int64(row.Count), //nolint:gosec // count() is domain-bounded
		}
	}
	return out
}
