package otlp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/auth"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type TeamResolver interface {
	ResolveTeamID(ctx context.Context, apiKey string) (int64, error)
}

type Queue interface {
	Enqueue(rows []ingest.Row) error
	Close() error
}

type Limiter interface {
	Allow(teamID int64, n int64) bool
}

type SizeTracker interface {
	Track(teamID int64, bytes int64)
}

type SharedDependencies struct {
	Authenticator TeamResolver
	Tracker       *ingest.ByteTracker
	Limiter       Limiter
}

var (
	sharedDepsOnce sync.Once
	sharedDeps     *SharedDependencies
)

func Shared(sqlDB *registry.SQLDB, cfg registry.AppConfig) *SharedDependencies {
	sharedDepsOnce.Do(func() {
		var rdb *redis.Client
		if cfg.Redis.Enabled {
			rdb = redis.NewClient(&redis.Options{
				Addr: fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port),
			})
		}
		sharedDeps = &SharedDependencies{
			Authenticator: auth.NewAuthenticator(sqlDB),
			Tracker:       ingest.NewByteTracker(sqlDB, rdb, time.Hour),
			Limiter:       ingest.NewTeamLimiter(ingest.DefaultTeamRatePerSec, ingest.DefaultTeamBurstRows),
		}
	})
	return sharedDeps
}

func QueueOpts(appConfig registry.AppConfig) []ingest.Option {
	opts := []ingest.Option{
		ingest.WithBatchSize(appConfig.Queue.BatchSize),
		ingest.WithFlushInterval(int(appConfig.Queue.FlushIntervalMs)),
	}
	if appConfig.Queue.Capacity > 0 {
		opts = append(opts, ingest.WithCapacity(appConfig.Queue.Capacity))
	}
	return opts
}

func ResolveTeamID(ctx context.Context, resolver TeamResolver) (int64, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, status.Error(codes.Unauthenticated, "missing metadata")
	}

	keys := md.Get("x-api-key")
	if len(keys) == 0 {
		return 0, status.Error(codes.Unauthenticated, "missing x-api-key metadata header")
	}

	teamID, err := resolver.ResolveTeamID(ctx, keys[0])
	if err != nil {
		if errors.Is(err, auth.ErrMissingAPIKey) || errors.Is(err, auth.ErrInvalidAPIKey) {
			return 0, status.Error(codes.Unauthenticated, err.Error())
		}
		return 0, status.Error(codes.Internal, err.Error())
	}
	return teamID, nil
}

func TrackPayloadSize(tracker SizeTracker, teamID int64, msg proto.Message) {
	if tracker == nil || teamID <= 0 || msg == nil {
		return
	}
	size := proto.Size(msg)
	if size <= 0 {
		return
	}
	tracker.Track(teamID, int64(size))
}

type Lifecycle struct {
	shared *SharedDependencies
}

func NewLifecycle(shared *SharedDependencies) Lifecycle {
	return Lifecycle{shared: shared}
}

func (l Lifecycle) Stop() {
	if l.shared != nil && l.shared.Tracker != nil {
		l.shared.Tracker.Stop()
	}
}
