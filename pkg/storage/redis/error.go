package redis

import "fmt"

// WrapRedisPingError annotates Redis connectivity check failures.
func WrapRedisPingError(err error) error {
	return fmt.Errorf("ping redis: %w", err)
}
