package hosts

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// GetHosts returns hosts with CPU/mem/disk utilization and saturation scores,
// optionally narrowed and enriched with RED traffic for a service.
func (s *Service) GetHosts(ctx context.Context, teamID, startMs, endMs int64, serviceName string) ([]Host, error) {
	util, err := s.repo.QueryHostUtilization(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	byHost, order := foldUtilization(util)

	if serviceName == "" {
		out := make([]Host, 0, len(order))
		for _, host := range order {
			out = append(out, byHost[host])
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Saturation > out[j].Saturation })
		return out, nil
	}

	spans, err := s.repo.QueryHostSpans(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return enrichWithSpans(byHost, spans, endMs-startMs), nil
}

// foldUtilization folds the (host, metric) rows into one saturation Host per
// host, preserving first-seen order.
func foldUtilization(rows []hostMetricRow) (map[string]Host, []string) {
	metrics := map[string]map[string]float64{}
	order := []string{}
	for _, r := range rows {
		if _, ok := metrics[r.Host]; !ok {
			metrics[r.Host] = map[string]float64{}
			order = append(order, r.Host)
		}
		metrics[r.Host][r.MetricName] = r.Value
	}

	byHost := make(map[string]Host, len(order))
	for _, host := range order {
		m := metrics[host]
		cpu := valueOrZero(foldCPU(m))
		mem := valueOrZero(foldMem(m))
		disk := valueOrZero(foldDisk(m))
		sat := math.Max(cpu, math.Max(mem, disk))
		byHost[host] = Host{
			Host:       host,
			Subsystem:  subsystemForHost(host),
			CPU:        cpu,
			Mem:        mem,
			Disk:       disk,
			Saturation: sat,
			Tone:       toneForSaturation(sat),
		}
	}
	return byHost, order
}

// enrichWithSpans folds RED traffic onto hosts running the service,
// ranking them by request volume.
func enrichWithSpans(byHost map[string]Host, spans []hostSpansRow, windowMs int64) []Host {
	durationSec := float64(windowMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}
	out := make([]Host, 0, len(spans))
	for _, row := range spans {
		h := byHost[row.Host] // zero saturation if the host has no metrics
		h.Host = row.Host

		total := int64(row.RequestCount)
		errs := int64(row.ErrorCount)
		errRate := 0.0
		if total > 0 {
			errRate = float64(errs) / float64(total)
		}
		rps := float64(total) / durationSec
		p99 := utils.SanitizeFloat(float64(row.P99Ms))

		h.Zone = row.Zone
		h.RPS = &rps
		h.ErrorRate = &errRate
		h.P99Ms = &p99
		h.Status = classifyHost(errRate, float64(row.P99Ms))
		h.LastSeen = row.LastSeen.Format(time.RFC3339)
		h.RequestCount = total
		h.ErrorCount = errs
		out = append(out, h)
	}
	return out
}

// classifyHost categorizes a host status (error/warn/healthy) based on
// error rate and p99 latency thresholds.
func classifyHost(errRate, p99Ms float64) HostStatus {
	if errRate >= 0.10 || p99Ms >= 2000 {
		return HostError
	}
	if errRate >= 0.02 || p99Ms >= 1000 {
		return HostWarn
	}
	return HostHealthy
}

// Folds mirroring the infra cpu/memory/disk modules.

func foldCPU(m map[string]float64) *float64 {
	return averagePresent(m,
		infraconsts.MetricSystemCPUUtilization,
		infraconsts.MetricSystemCPUUsage,
		infraconsts.MetricProcessCPUUsage,
	)
}

func foldMem(m map[string]float64) *float64 {
	var values []float64
	if v, ok := m[infraconsts.MetricSystemMemoryUtilization]; ok {
		if nv := normalizeUtilization(v); nv != nil {
			values = append(values, *nv)
		}
	}
	if max := m[infraconsts.MetricJVMMemoryMax]; max > 0 {
		values = append(values, infraconsts.PercentageMultiplier*m[infraconsts.MetricJVMMemoryUsed]/max)
	}
	return average(values)
}

func foldDisk(m map[string]float64) *float64 {
	// Only system.disk.utilization is a percentage; raw bytes are rejected.
	return averagePresent(m, infraconsts.MetricSystemDiskUtilization)
}

func averagePresent(m map[string]float64, metricNames ...string) *float64 {
	var values []float64
	for _, name := range metricNames {
		if v, ok := m[name]; ok {
			if nv := normalizeUtilization(v); nv != nil {
				values = append(values, *nv)
			}
		}
	}
	return average(values)
}

// normalizeUtilization normalizes percentages and rejects NaN/Inf, negatives,
// and raw byte counts.
func normalizeUtilization(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > infraconsts.PercentageThreshold*100 {
		return nil
	}
	if v <= infraconsts.PercentageThreshold {
		v *= infraconsts.PercentageMultiplier
	}
	return &v
}

func average(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	avg := sum / float64(len(values))
	return &avg
}

func valueOrZero(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}
