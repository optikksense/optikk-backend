package interfaces

import (
	"github.com/observability/observability-backend-go/internal/modules/insights/model"
)

// Service encapsulates the business logic for the insights module.
type Service interface {
	GetInsightSloSli(teamUUID string, startMs, endMs int64, serviceName string) (*model.SloSliResponse, error)
	GetInsightLogsStream(teamUUID string, startMs, endMs int64, limit int) (*model.LogsStreamResponse, error)
}
