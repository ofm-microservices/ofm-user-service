package repository

import (
	"errors"
	"fmt"
)

var (
	ErrNilRedisClient = errors.New("redis client is nil")
	ErrNilUser        = errors.New("user is nil")
	ErrNilLogger      = errors.New("logger is nil")
)

// WrapMarshalUserCacheError annotates cache serialization failures.
func WrapMarshalUserCacheError(err error) error {
	return fmt.Errorf("marshal user cache: %w", err)
}

// WrapSetUserCacheError annotates Redis upsert failures for the user cache.
func WrapSetUserCacheError(key string, err error) error {
	return fmt.Errorf("set user cache by key %q: %w", key, err)
}

// WrapDeleteUserCacheError annotates Redis delete failures for the user cache.
func WrapDeleteUserCacheError(key string, err error) error {
	return fmt.Errorf("delete user cache by key %q: %w", key, err)
}

// WrapGetUserCacheError annotates Redis read failures for the user cache.
func WrapGetUserCacheError(key string, err error) error {
	return fmt.Errorf("get user cache by key %q: %w", key, err)
}

// WrapUnmarshalUserCacheError annotates cache decoding failures.
func WrapUnmarshalUserCacheError(err error) error {
	return fmt.Errorf("unmarshal user cache: %w", err)
}
