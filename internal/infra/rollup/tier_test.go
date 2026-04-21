package rollup

import "testing"

func TestPickTier(t *testing.T) {
	const hour = int64(3_600_000)
	cases := []struct {
		name     string
		spanMs   int64
		wantTier Tier
	}{
		{"1 minute", 60_000, Tier1m},
		{"3 hours exact", 3 * hour, Tier1m},
		{"3 hours + 1 min", 3*hour + 60_000, Tier5m},
		{"12 hours", 12 * hour, Tier5m},
		{"24 hours exact", 24 * hour, Tier5m},
		{"24 hours + 1 min", 24*hour + 60_000, Tier1h},
		{"7 days", 7 * 24 * hour, Tier1h},
		{"30 days", 30 * 24 * hour, Tier1h},
		{"90 days", 90 * 24 * hour, Tier1h},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := PickTier(0, c.spanMs)
			if got != c.wantTier {
				t.Fatalf("PickTier(%d) = %+v, want %+v", c.spanMs, got, c.wantTier)
			}
		})
	}
}

func TestTierTableFor(t *testing.T) {
	const hour = int64(3_600_000)
	table, step := TierTableFor("observability.spans_rollup", 0, 2*hour)
	if table != "observability.spans_rollup_1m" || step != 1 {
		t.Fatalf("2h range: got (%q, %d), want (observability.spans_rollup_1m, 1)", table, step)
	}
	table, step = TierTableFor("observability.spans_rollup", 0, 12*hour)
	if table != "observability.spans_rollup_5m" || step != 5 {
		t.Fatalf("12h range: got (%q, %d), want (observability.spans_rollup_5m, 5)", table, step)
	}
	table, step = TierTableFor("observability.spans_rollup", 0, 7*24*hour)
	if table != "observability.spans_rollup_1h" || step != 60 {
		t.Fatalf("7d range: got (%q, %d), want (observability.spans_rollup_1h, 60)", table, step)
	}
}
