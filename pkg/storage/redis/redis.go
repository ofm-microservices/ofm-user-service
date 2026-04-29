package redis

import (
	"context"
	"fmt"
	"user-service/config"

	"github.com/redis/go-redis/v9"
)

// Open creates the Redis client used for the user read model and verifies the
// connection with a ping.
func Open(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, WrapRedisPingError(err)
	}

	return rdb, nil
}
