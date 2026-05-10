package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	domain "user-service/internal/domain"
	"user-service/internal/infra/read/redis/mapper"

	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/redis/go-redis/v9"
)

type repo struct {
	rdb *redis.Client
}

// New constructs the Redis-backed user read-model repository.
func New(rdb *redis.Client) (domain.UserReadRepository, error) {
	if rdb == nil {
		return nil, ErrNilRedisClient
	}

	return &repo{rdb: rdb}, nil
}

func (r *repo) Upsert(ctx context.Context, user *domain.User) error {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("set", "user", status, time.Since(started)) }()

	if user == nil {
		status = "error"
		return ErrNilUser
	}

	cache := mapper.MapDomainUserToCache(user)
	payload, err := json.Marshal(cache)
	if err != nil {
		status = "error"
		return WrapMarshalUserCacheError(err)
	}

	key := UserCacheKey(user.ID)
	if err := r.rdb.Set(ctx, key, payload, 0).Err(); err != nil {
		status = "error"
		return WrapSetUserCacheError(key, err)
	}

	return nil
}

func (r *repo) DeleteByID(ctx context.Context, userID string) error {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("del", "user", status, time.Since(started)) }()

	key := UserCacheKey(userID)
	if err := r.rdb.Del(ctx, key).Err(); err != nil {
		status = "error"
		return WrapDeleteUserCacheError(key, err)
	}

	return nil
}

// UserCacheKey builds the Redis key used for the user read model.
func UserCacheKey(userID string) string {
	return fmt.Sprintf("user:%s", userID)
}
