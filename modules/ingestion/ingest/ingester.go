package ingest

import (
	ingestimpl "github.com/observability/observability-backend-go/modules/ingestion/ingest/impl"
	ingestinterfaces "github.com/observability/observability-backend-go/modules/ingestion/ingest/interfaces"
)

type Ingester = ingestinterfaces.Ingester

type DirectIngester = ingestimpl.DirectIngester
type KafkaIngester = ingestimpl.KafkaIngester
type KafkaConsumer = ingestimpl.KafkaConsumer
type KafkaConsumerConfig = ingestimpl.KafkaConsumerConfig

const (
	TopicSpans   = ingestimpl.TopicSpans
	TopicMetrics = ingestimpl.TopicMetrics
	TopicLogs    = ingestimpl.TopicLogs
)

var NewDirectIngester = ingestimpl.NewDirectIngester
var NewKafkaIngester = ingestimpl.NewKafkaIngester
var NewKafkaConsumer = ingestimpl.NewKafkaConsumer
