package errors

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
			ErrorCount:      row.ErrorCount,
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
		ErrorCount:      row.ErrorCount,
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
			DurationMs: row.DurationMs,
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
			ErrorCount: row.ErrorCount,
		}
	}
	return points
}

func mapServiceErrorRateRows(rows []serviceErrorRateRow) []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			ServiceName:  row.ServiceName,
			Timestamp:    row.Timestamp,
			RequestCount: row.RequestCount,
			ErrorCount:   row.ErrorCount,
			ErrorRate:    row.ErrorRate,
			AvgLatency:   row.AvgLatency,
		}
	}
	return points
}

func mapErrorVolumeRows(rows []errorVolumeRow) []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			ServiceName: row.ServiceName,
			Timestamp:   row.Timestamp,
			ErrorCount:  row.ErrorCount,
		}
	}
	return points
}

func mapLatencyErrorRows(rows []latencyErrorRow) []TimeSeriesPoint {
	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			ServiceName:  row.ServiceName,
			Timestamp:    row.Timestamp,
			RequestCount: row.RequestCount,
			ErrorCount:   row.ErrorCount,
			AvgLatency:   row.AvgLatency,
		}
	}
	return points
}
