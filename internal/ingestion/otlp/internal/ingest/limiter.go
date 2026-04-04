package ingest

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	limiterlib "github.com/ulule/limiter/v3"
	memorylimiter "github.com/ulule/limiter/v3/drivers/store/memory"
)

const (
	DefaultTeamBurstRows  = int64(100_000)
	DefaultTeamRatePerSec = int64(100_000)
)

type TeamLimiter struct {
	limiter *limiterlib.Limiter
}

func NewTeamLimiter(ratePerSec, burst int64) *TeamLimiter {
	rate := limiterlib.Rate{
		Period: 1 * time.Second,
		Limit:  burst,
	}

	store := memorylimiter.NewStore()
	return &TeamLimiter{
		limiter: limiterlib.New(store, rate),
	}
}

func (l *TeamLimiter) Allow(teamID int64, n int64) bool {
	ctx := context.Background()
	key := strconv.FormatInt(teamID, 10)

	limitContext, err := l.limiter.Increment(ctx, key, n)
	if err != nil {
		slog.Error("ingest: internal rate limiter error", slog.Any("error", err))
		return false
	}

	if limitContext.Reached {
		slog.Warn("ingest: rate limit exceeded",
			slog.Int64("team_id", teamID),
			slog.Int64("want", n),
		)
		return false
	}

	return true
}
