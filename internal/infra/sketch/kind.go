package sketch

import "time"

// Kind identifies a family of sketches (distribution or cardinality) keyed by
// a fixed dimension tuple and time bucket. Each Kind has a stable wire id that
// shows up in Redis keys — add new kinds by appending, never by renumbering.
type Kind struct {
	// ID is the short stable key segment for Redis keys. Must be unique.
	ID string
	// Family is either FamilyDistribution (t-digest) or FamilyCardinality (HLL).
	Family Family
	// Bucket is the time granularity at which Observe() folds samples together.
	Bucket time.Duration
	// TTL controls how long a sketch lives in Redis after its bucket closes.
	TTL time.Duration
}

type Family int

const (
	FamilyDistribution Family = iota + 1
	FamilyCardinality
)

// Distribution sketch kinds (t-digest).
var (
	SpanLatencyService = Kind{
		ID:     "sls",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    15 * 24 * time.Hour,
	}
	SpanLatencyEndpoint = Kind{
		ID:     "sle",
		Family: FamilyDistribution,
		Bucket: 5 * time.Minute,
		TTL:    15 * 24 * time.Hour,
	}
	DbOpLatency = Kind{
		ID:     "dbol",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    15 * 24 * time.Hour,
	}
	KafkaTopicLatency = Kind{
		ID:     "ktl",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    15 * 24 * time.Hour,
	}
)

// Cardinality sketch kinds (HyperLogLog).
var (
	NodePodCount = Kind{
		ID:     "npc",
		Family: FamilyCardinality,
		Bucket: time.Minute,
		TTL:    15 * 24 * time.Hour,
	}
	AiTraceCount = Kind{
		ID:     "atc",
		Family: FamilyCardinality,
		Bucket: time.Minute,
		TTL:    15 * 24 * time.Hour,
	}
)

// AllKinds is the canonical registry used by the aggregator and store.
var AllKinds = []Kind{
	SpanLatencyService,
	SpanLatencyEndpoint,
	DbOpLatency,
	KafkaTopicLatency,
	NodePodCount,
	AiTraceCount,
}
