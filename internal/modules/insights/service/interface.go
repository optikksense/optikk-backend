package service

import (
	"github.com/observability/observability-backend-go/internal/modules/insights/model"
)

// Service encapsulates the business logic for the insights module.
type Service interface {
	GetInsightResourceUtilization(teamUUID string, startMs, endMs int64) (*model.ResourceUtilizationResponse, error)
	GetInsightSloSli(teamUUID string, startMs, endMs int64, serviceName string) (*model.SloSliResponse, error)
	GetInsightLogsStream(teamUUID string, startMs, endMs int64, limit int) (*model.LogsStreamResponse, error)
	GetInsightDatabaseCache(teamUUID string, startMs, endMs int64) (*model.DatabaseCacheResponse, error)
	GetInsightMessagingQueue(teamUUID string, startMs, endMs int64) (*model.MessagingQueueResponse, error)
}
