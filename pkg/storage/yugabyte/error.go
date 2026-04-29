package db

import "fmt"

// WrapResolveMigrationsPathError annotates relative migration path resolution
// failures.
func WrapResolveMigrationsPathError(err error) error {
	return fmt.Errorf("resolve migrations path: %w", err)
}

// WrapCreateMigratorError annotates migrate client construction failures.
func WrapCreateMigratorError(err error) error {
	return fmt.Errorf("create migrator: %w", err)
}

// WrapRunMigrationsError annotates migration execution failures.
func WrapRunMigrationsError(err error) error {
	return fmt.Errorf("run migrations: %w", err)
}

// WrapOpenDBError annotates SQL driver connection failures.
func WrapOpenDBError(err error) error {
	return fmt.Errorf("open db: %w", err)
}

// WrapPingDBError annotates health-check ping failures.
func WrapPingDBError(err error) error {
	return fmt.Errorf("ping db: %w", err)
}
