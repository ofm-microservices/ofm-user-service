package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	domain "user-service/internal/domain"
	"user-service/internal/infra/read/redis/mapper"
	"user-service/internal/infra/read/redis/model"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/redis/go-redis/v9"
)

type repo struct {
	rdb *redis.Client
	log logging.Logger
}

// New constructs the Redis-backed user read-model repository.
func New(rdb *redis.Client, log logging.Logger) (domain.UserReadRepository, error) {
	if rdb == nil {
		return nil, ErrNilRedisClient
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &repo{rdb: rdb, log: log.With(logging.String("module", "redis-repository"))}, nil
}

func (r *repo) Upsert(ctx context.Context, user *domain.User) error {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("set", "user", status, time.Since(started)) }()

	if user == nil {
		status = "error"
		r.log.Error("upsert user failed",
			logging.Operation("redis.user.upsert"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.Err(ErrNilUser),
		)
		return ErrNilUser
	}

	cache := mapper.MapDomainUserToCache(user)
	payload, err := json.Marshal(cache)
	if err != nil {
		status = "error"
		r.log.Error("marshal user cache failed",
			logging.Operation("redis.user.upsert"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", user.ID),
			logging.Err(err),
		)
		return WrapMarshalUserCacheError(err)
	}

	key := UserPreviewCacheKey(user.ID)
	if err := r.rdb.Set(ctx, key, payload, 0).Err(); err != nil {
		status = "error"
		r.log.Error("set user cache failed",
			logging.Operation("redis.user.upsert"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("key", key),
			logging.Err(err),
		)
		return WrapSetUserCacheError(key, err)
	}

	return nil
}

func (r *repo) UpsertByUsername(ctx context.Context, user *domain.User) error {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("set", "user_detailed", status, time.Since(started)) }()

	if user == nil {
		status = "error"
		r.log.Error("upsert detailed user failed",
			logging.Operation("redis.user_detailed.upsert"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.Err(ErrNilUser),
		)
		return ErrNilUser
	}

	cache := mapper.MapDomainUserToDetailedCache(user)
	payload, err := json.Marshal(cache)
	if err != nil {
		status = "error"
		r.log.Error("marshal detailed user cache failed",
			logging.Operation("redis.user_detailed.upsert"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.String("username", user.Username),
			logging.Err(err),
		)
		return WrapMarshalUserCacheError(err)
	}

	key := UserDetailedCacheKey(user.Username)
	if err := r.rdb.Set(ctx, key, payload, 0).Err(); err != nil {
		status = "error"
		r.log.Error("set detailed user cache failed",
			logging.Operation("redis.user_detailed.upsert"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("key", key),
			logging.Err(err),
		)
		return WrapSetUserCacheError(key, err)
	}

	return nil
}

func (r *repo) DeleteByID(ctx context.Context, userID string) error {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("del", "user", status, time.Since(started)) }()

	key := UserPreviewCacheKey(userID)
	if err := r.rdb.Del(ctx, key).Err(); err != nil {
		status = "error"
		r.log.Error("delete user cache failed",
			logging.Operation("redis.user.delete_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("key", key),
			logging.Err(err),
		)
		return WrapDeleteUserCacheError(key, err)
	}

	return nil
}

func (r *repo) DeleteByUsername(ctx context.Context, username string) error {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("del", "user_detailed", status, time.Since(started)) }()

	key := UserDetailedCacheKey(username)
	if err := r.rdb.Del(ctx, key).Err(); err != nil {
		status = "error"
		r.log.Error("delete detailed user cache failed",
			logging.Operation("redis.user_detailed.delete_by_username"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("key", key),
			logging.Err(err),
		)
		return WrapDeleteUserCacheError(key, err)
	}

	return nil
}

func (r *repo) GetByID(ctx context.Context, userID string) (*domain.User, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("get", "user", status, time.Since(started)) }()

	key := UserPreviewCacheKey(userID)
	raw, err := r.rdb.Get(ctx, key).Bytes()
	if err != nil {
		status = "error"
		if err == redis.Nil {
			return nil, domain.ErrUserNotFound
		}
		r.log.Error("get user cache failed",
			logging.Operation("redis.user.get_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("key", key),
			logging.Err(err),
		)
		return nil, WrapGetUserCacheError(key, err)
	}

	var cache model.UserCache
	if err := json.Unmarshal(raw, &cache); err != nil {
		status = "error"
		r.log.Error("unmarshal user cache failed",
			logging.Operation("redis.user.get_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("key", key),
			logging.Err(err),
		)
		return nil, WrapUnmarshalUserCacheError(err)
	}

	return mapper.MapCacheToDomainUser(cache), nil
}

func (r *repo) GetByUsername(ctx context.Context, username string) (*domain.User, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveRedis("get", "user_detailed", status, time.Since(started)) }()

	key := UserDetailedCacheKey(username)
	raw, err := r.rdb.Get(ctx, key).Bytes()
	if err != nil {
		status = "error"
		if err == redis.Nil {
			return nil, domain.ErrUserNotFound
		}
		r.log.Error("get detailed user cache failed",
			logging.Operation("redis.user_detailed.get_by_username"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("key", key),
			logging.Err(err),
		)
		return nil, WrapGetUserCacheError(key, err)
	}

	var cache model.UserDetailedCache
	if err := json.Unmarshal(raw, &cache); err != nil {
		status = "error"
		r.log.Error("unmarshal detailed user cache failed",
			logging.Operation("redis.user_detailed.get_by_username"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("key", key),
			logging.Err(err),
		)
		return nil, WrapUnmarshalUserCacheError(err)
	}

	return mapper.MapDetailedCacheToDomainUser(cache), nil
}

// UserPreviewCacheKey builds the Redis key used for the user preview read model.
func UserPreviewCacheKey(userID string) string {
	return fmt.Sprintf("user:preview:%s", userID)
}

// UserDetailedCacheKey builds the Redis key used for the detailed public user
// read model.
func UserDetailedCacheKey(username string) string {
	return fmt.Sprintf("user:detailed:%s", username)
}
