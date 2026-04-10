package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	redigoredis "github.com/gomodule/redigo/redis"
	goredis "github.com/redis/go-redis/v9"
)

type Clients struct {
	Client *goredis.Client
	Pool   *redigoredis.Pool
}

func NewClients(cfg config.Config) (*Clients, error) {
	if !cfg.Redis.Enabled {
		return nil, fmt.Errorf("redis must be enabled")
	}

	client := goredis.NewClient(&goredis.Options{
		Addr:     cfg.RedisAddr(),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	pool := &redigoredis.Pool{
		MaxIdle:     10,
		IdleTimeout: 5 * time.Minute,
		Dial: func() (redigoredis.Conn, error) {
			options := []redigoredis.DialOption{
				redigoredis.DialDatabase(cfg.Redis.DB),
			}
			if cfg.Redis.Password != "" {
				options = append(options, redigoredis.DialPassword(cfg.Redis.Password))
			}
			return redigoredis.Dial("tcp", cfg.RedisAddr(), options...)
		},
		TestOnBorrow: func(c redigoredis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	return &Clients{
		Client: client,
		Pool:   pool,
	}, nil
}
