package hosts

import (
	"context"
	"errors"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"golang.org/x/sync/errgroup"
)

type Service interface {
	GetHostsForService(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]HostForService, error)
}

type HostsService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &HostsService{repo: repo}
}

// GetHostsForService fans out one spans query (RED per host) and two metric
// queries (CPU, memory) in parallel and joins them by host.
func (s *HostsService) GetHostsForService(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string,
) ([]HostForService, error) {
	if serviceName == "" {
		return nil, errors.New("serviceName is required")
	}
	spans, cpu, mem, err := s.fanOut(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return buildHostCards(spans, cpu, mem, endMs-startMs), nil
}

func (s *HostsService) fanOut(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string,
) ([]hostSpansRow, []hostMetricRow, []hostMetricRow, error) {
	var (
		spans []hostSpansRow
		cpu   []hostMetricRow
		mem   []hostMetricRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rows, err := s.repo.QueryHostSpans(gctx, teamID, startMs, endMs, serviceName)
		spans = rows
		return err
	})
	g.Go(func() error {
		rows, err := s.repo.QueryHostMetric(gctx, teamID, startMs, endMs, CPUMetricName())
		cpu = rows
		return err
	})
	g.Go(func() error {
		rows, err := s.repo.QueryHostMetric(gctx, teamID, startMs, endMs, MemoryMetricName())
		mem = rows
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, nil, nil, err
	}
	return spans, cpu, mem, nil
}

func buildHostCards(spans []hostSpansRow, cpu, mem []hostMetricRow, windowMs int64) []HostForService {
	cpuByHost := metricByHost(cpu)
	memByHost := metricByHost(mem)
	durationSec := float64(windowMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}
	out := make([]HostForService, len(spans))
	for i, row := range spans {
		out[i] = toHostCard(row, cpuByHost[row.Host], memByHost[row.Host], durationSec)
	}
	return out
}

func metricByHost(rows []hostMetricRow) map[string]float64 {
	out := make(map[string]float64, len(rows))
	for _, row := range rows {
		out[row.Host] = utils.SanitizeFloat(row.Value)
	}
	return out
}

func toHostCard(row hostSpansRow, cpu, mem float64, durationSec float64) HostForService {
	total := int64(row.RequestCount) //nolint:gosec // domain-bounded
	errs := int64(row.ErrorCount)    //nolint:gosec // domain-bounded
	errRate := 0.0
	if total > 0 {
		errRate = float64(errs) / float64(total)
	}
	return HostForService{
		Host:         row.Host,
		Zone:         row.Zone,
		CPUPct:       optPct(cpu),
		MemPct:       optPct(mem),
		RPS:          float64(total) / durationSec,
		ErrorRate:    errRate,
		P99Ms:        utils.SanitizeFloat(float64(row.P99Ms)),
		Status:       classifyHost(errRate, float64(row.P99Ms)),
		LastSeen:     row.LastSeen.Format(time.RFC3339),
		RequestCount: total,
		ErrorCount:   errs,
	}
}

// optPct returns a pointer to the percent value when present, or nil so the
// JSON omits the field — matching the design's "show — when missing" rule.
func optPct(v float64) *float64 {
	if v == 0 {
		return nil
	}
	pct := v
	if pct <= 1 {
		pct *= 100
	}
	return &pct
}

// classifyHost replicates the design's red/yellow/green dot scheme:
// >=10% errors or >=2s p99 → error; >=2% errors or >=1s p99 → warn; else healthy.
func classifyHost(errRate, p99Ms float64) HostStatus {
	if errRate >= 0.10 || p99Ms >= 2000 {
		return HostError
	}
	if errRate >= 0.02 || p99Ms >= 1000 {
		return HostWarn
	}
	return HostHealthy
}
