// Round-trip tests asserting writer-stored bucket value falls inside reader-computed [bucketStart, bucketEnd] for any timestamp.
package timebucket

import (
	"testing"
	"time"
)

func TestSpansBucketRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		ts   time.Time
	}{
		{"interior", time.Date(2026, 4, 27, 12, 7, 30, 0, time.UTC)},
		{"on bucket boundary", time.Date(2026, 4, 27, 12, 5, 0, 0, time.UTC)},
		{"epoch", time.Unix(0, 0).UTC()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ts := c.ts.Unix()
			written := SpansBucketStart(ts)
			qStart := SpansBucketStart(ts - 60)
			qEnd := SpansBucketStart(ts + 60)
			if written < qStart || written > qEnd {
				t.Fatalf("spans round-trip failed: written=%d qStart=%d qEnd=%d ts=%v", written, qStart, qEnd, c.ts)
			}
		})
	}
}

func TestSpansBucketWindowNarrowerThanGrain(t *testing.T) {
	ts := time.Date(2026, 4, 27, 12, 7, 30, 0, time.UTC).Unix()
	written := SpansBucketStart(ts)
	qStart := SpansBucketStart(ts - 15)
	qEnd := SpansBucketStart(ts + 15)
	if qStart != qEnd {
		t.Fatalf("expected start==end for narrow window: qStart=%d qEnd=%d", qStart, qEnd)
	}
	if written != qStart {
		t.Fatalf("written=%d not in [%d, %d]", written, qStart, qEnd)
	}
}

func TestLogsBucketRoundTrip(t *testing.T) {
	ts := time.Date(2026, 4, 27, 12, 7, 30, 0, time.UTC).Unix()
	written := LogsBucketStart(ts)
	qStart := LogsBucketStart(ts - 60)
	qEnd := LogsBucketStart(ts + 60)
	if written < qStart || written > qEnd {
		t.Fatalf("logs round-trip failed: written=%d qStart=%d qEnd=%d", written, qStart, qEnd)
	}
}

func TestMetricsHourBucketRoundTrip(t *testing.T) {
	ts := time.Date(2026, 4, 27, 5, 30, 0, 0, time.UTC).Unix()
	stored := MetricsHourBucket(ts)
	qStart := MetricsHourBucket(ts - 600)
	qEnd := MetricsHourBucket(ts + 600).Add(time.Hour)
	if stored.Before(qStart) || !stored.Before(qEnd) {
		t.Fatalf("stored=%v not in [%v, %v)", stored, qStart, qEnd)
	}
}

func TestDisplayBucketGrainSelection(t *testing.T) {
	cases := []struct {
		name     string
		windowMs int64
		want     time.Duration
	}{
		{"1h → 1m", 3600 * 1000, time.Minute},
		{"3h boundary → 1m", 3 * 3600 * 1000, time.Minute},
		{"3h+1ms → 5m", 3*3600*1000 + 1, 5 * time.Minute},
		{"24h boundary → 5m", 24 * 3600 * 1000, 5 * time.Minute},
		{"24h+1ms → 1h", 24*3600*1000 + 1, time.Hour},
		{"7d boundary → 1h", 7 * 24 * 3600 * 1000, time.Hour},
		{"7d+1ms → 1d", 7*24*3600*1000 + 1, 24 * time.Hour},
	}
	for _, c := range cases {
		if got := DisplayGrain(c.windowMs); got != c.want {
			t.Errorf("%s: DisplayGrain(%d) = %v, want %v", c.name, c.windowMs, got, c.want)
		}
	}
}

func TestDisplayBucketAlignment(t *testing.T) {
	t.Run("two ts in same 5m bucket", func(t *testing.T) {
		windowMs := int64(24 * 3600 * 1000) // → 5m grain
		t1 := time.Date(2026, 4, 27, 12, 7, 30, 0, time.UTC).Unix()
		t2 := time.Date(2026, 4, 27, 12, 9, 59, 0, time.UTC).Unix()
		b1 := DisplayBucket(t1, windowMs)
		b2 := DisplayBucket(t2, windowMs)
		if !b1.Equal(b2) {
			t.Fatalf("expected same bucket, got %v and %v", b1, b2)
		}
	})

	t.Run("two ts straddling boundary fall in different buckets", func(t *testing.T) {
		windowMs := int64(24 * 3600 * 1000)
		t1 := time.Date(2026, 4, 27, 12, 4, 59, 0, time.UTC).Unix()
		t2 := time.Date(2026, 4, 27, 12, 5, 0, 0, time.UTC).Unix()
		b1 := DisplayBucket(t1, windowMs)
		b2 := DisplayBucket(t2, windowMs)
		if b1.Equal(b2) {
			t.Fatalf("expected different buckets across boundary, got %v == %v", b1, b2)
		}
		if b2.Sub(b1) != 5*time.Minute {
			t.Fatalf("expected 5m gap between buckets, got %v", b2.Sub(b1))
		}
	})
}
