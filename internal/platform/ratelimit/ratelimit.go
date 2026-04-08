package ratelimit

import (
	"fmt"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	middleware "github.com/Optikk-Org/optikk-backend/internal/infra/middleware"
)

const (
	ProviderLocal = "local"
)

type Limiter interface {
	Allow(key string) bool
}

func New(cfg config.Config, rate, burst int, window time.Duration) (Limiter, error) {
	switch normalizeProvider(cfg.RateLimiterProvider()) {
	case ProviderLocal:
		return middleware.NewRateLimiter(rate, burst, window), nil
	default:
		return nil, fmt.Errorf("unsupported rate limiter provider %q", cfg.RateLimiterProvider())
	}
}

func normalizeProvider(provider string) string {
	provider = strings.TrimSpace(strings.ToLower(provider))
	if provider == "" {
		return ProviderLocal
	}
	return provider
}
