// Package quantile interpolates quantiles from explicit-bucket histograms.
package quantile

// FromHistogram returns the q-th quantile (0..1) over an OTLP-style explicit-bucket histogram —
// linear interpolation across cumulative bucket counts. Transcribed from prometheus/promql/quantile.go BucketQuantile.
func FromHistogram(buckets []float64, counts []uint64, q float64) float64 {
	if len(buckets) == 0 || len(counts) == 0 || len(buckets) != len(counts) {
		return 0
	}
	var total uint64
	for _, c := range counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := q * float64(total)
	var cum uint64
	for i, c := range counts {
		cum += c
		if float64(cum) < target {
			continue
		}
		hi := buckets[i]
		lo := 0.0
		if i > 0 {
			lo = buckets[i-1]
		}
		bucketCount := float64(c)
		frac := 1.0
		if bucketCount > 0 {
			frac = 1.0 - (float64(cum)-target)/bucketCount
		}
		return lo + frac*(hi-lo)
	}
	return buckets[len(buckets)-1]
}
