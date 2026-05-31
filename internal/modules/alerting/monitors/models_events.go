package monitors

import (
	"time"
)

// MonitorEventResponse is the wire shape for monitor_events rows. The
// MonitorName is denormalized for the activity feed (saves a roundtrip).
type MonitorEventResponse struct {
	ID          int64      `json:"id"`
	MonitorID   int64      `json:"monitor_id"`
	MonitorName string     `json:"monitor_name"`
	Kind        string     `json:"kind"`
	Value       *float64   `json:"value,omitempty"`
	Threshold   *float64   `json:"threshold,omitempty"`
	PeakValue   *float64   `json:"peak_value,omitempty"`
	ResolvedBy  string     `json:"resolved_by,omitempty"`
	Note        string     `json:"note,omitempty"`
	StartedAt   time.Time  `json:"started_at"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`
}

// toEventResponses converts repository event rows into wire responses.
func toEventResponses(rows []EventRow) []MonitorEventResponse {
	out := make([]MonitorEventResponse, 0, len(rows))
	for _, r := range rows {
		ev := MonitorEventResponse{
			ID:          r.ID,
			MonitorID:   r.MonitorID,
			MonitorName: r.MonitorName,
			Kind:        r.Kind,
			StartedAt:   r.StartedAt,
		}
		if r.Value.Valid {
			v := r.Value.Float64
			ev.Value = &v
		}
		if r.Threshold.Valid {
			v := r.Threshold.Float64
			ev.Threshold = &v
		}
		if r.PeakValue.Valid {
			v := r.PeakValue.Float64
			ev.PeakValue = &v
		}
		if r.ResolvedBy.Valid {
			ev.ResolvedBy = r.ResolvedBy.String
		}
		if r.Note.Valid {
			ev.Note = r.Note.String
		}
		if r.EndedAt.Valid {
			t := r.EndedAt.Time
			ev.EndedAt = &t
		}
		out = append(out, ev)
	}
	return out
}
