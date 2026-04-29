package config

import "fmt"

// WrapParseEnvConfigError annotates env parsing failures from Config loading.
func WrapParseEnvConfigError(err error) error {
	return fmt.Errorf("parse env config: %w", err)
}
