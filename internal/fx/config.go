package appfx

import (
	"user-service/config"

	"go.uber.org/fx"
)

// ConfigModule provides parsed runtime configuration for user-service.
var ConfigModule = fx.Options(
	fx.Provide(ProvideConfig),
)

// ProvideConfig loads the service configuration from environment variables.
func ProvideConfig() (*config.Config, error) {
	return config.Load()
}
