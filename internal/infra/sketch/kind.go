package sketch

import "time"

// Kind identifies a family of sketches (distribution or cardinality) keyed by
// a fixed dimension tuple and time bucket. Each Kind has a stable wire id that
// shows up in Redis keys — add new kinds by appending, never by renumbering.
//
// IDs carry a _dd suffix: the wire format is DDSketch (via
// github.com/DataDog/sketches-go), not t-digest. Pre-DDSketch ids (`sls`,
// `sle`, `dbol`, `ktl`, `npc`, `atc`) were emitted briefly in PR #40; the
// suffix ensures the old keys age out of Redis without colliding with the
// new binary format.
type Kind struct {
	// ID is the short stable key segment for Redis keys. Must be unique.
	ID string
	// Family is either FamilyDistribution (DDSketch) or FamilyCardinality (HLL).
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

const defaultTTL = 15 * 24 * time.Hour

// Distribution sketch kinds (DDSketch).
var (
	SpanLatencyService = Kind{
		ID:     "sls_dd",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    defaultTTL,
	}
	SpanLatencyEndpoint = Kind{
		ID:     "sle_dd",
		Family: FamilyDistribution,
		Bucket: 5 * time.Minute,
		TTL:    defaultTTL,
	}
	DbOpLatency = Kind{
		ID:     "dbol_dd",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    defaultTTL,
	}
	KafkaTopicLatency = Kind{
		ID:     "ktl_dd",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    defaultTTL,
	}
	HttpServerDuration = Kind{
		ID:     "hsd_dd",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    defaultTTL,
	}
	HttpClientDuration = Kind{
		ID:     "hcd_dd",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    defaultTTL,
	}
	JvmMetricLatency = Kind{
		ID:     "jvm_dd",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    defaultTTL,
	}
	DbQueryLatency = Kind{
		ID:     "dbq_dd",
		Family: FamilyDistribution,
		Bucket: time.Minute,
		TTL:    defaultTTL,
	}
)

// Cardinality sketch kinds (HyperLogLog).
var (
	NodePodCount = Kind{
		ID:     "npc_dd",
		Family: FamilyCardinality,
		Bucket: time.Minute,
		TTL:    defaultTTL,
	}
	AiTraceCount = Kind{
		ID:     "atc_dd",
		Family: FamilyCardinality,
		Bucket: time.Minute,
		TTL:    defaultTTL,
	}
)

// AllKinds is the canonical registry used by the aggregator and store.
var AllKinds = []Kind{
	SpanLatencyService,
	SpanLatencyEndpoint,
	DbOpLatency,
	KafkaTopicLatency,
	HttpServerDuration,
	HttpClientDuration,
	JvmMetricLatency,
	DbQueryLatency,
	NodePodCount,
	AiTraceCount,
}
