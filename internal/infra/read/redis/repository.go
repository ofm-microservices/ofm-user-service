package repository

import (
	"context"
	"encoding/json"
	"fmt"
	domain "user-service/internal/domain"
	"user-service/internal/infra/read/redis/mapper"

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
	if user == nil {
		return ErrNilUser
	}

	cache := mapper.MapDomainUserToCache(user)
	payload, err := json.Marshal(cache)
	if err != nil {
		return WrapMarshalUserCacheError(err)
	}

	key := UserCacheKey(user.ID)
	if err := r.rdb.Set(ctx, key, payload, 0).Err(); err != nil {
		return WrapSetUserCacheError(key, err)
	}

	return nil
}

func (r *repo) DeleteByID(ctx context.Context, userID string) error {
	key := UserCacheKey(userID)
	if err := r.rdb.Del(ctx, key).Err(); err != nil {
		return WrapDeleteUserCacheError(key, err)
	}

	return nil
}

// UserCacheKey builds the Redis key used for the user read model.
func UserCacheKey(userID string) string {
	return fmt.Sprintf("user:%s", userID)
}
