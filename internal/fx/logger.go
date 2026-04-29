package appfx

import (
	"context"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	"user-service/config"

	"go.uber.org/fx"
)

// LoggerModule provides the structured logger used across user-service.
var LoggerModule = fx.Options(
	fx.Provide(ProvideLogger),
)

// ProvideLogger builds the service logger from runtime configuration.
func ProvideLogger(lc fx.Lifecycle, cfg *config.Config) (logging.Logger, error) {
	lg, err := logging.New("user-service", cfg.App.Env, cfg.App.LogLevel)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return lg.Sync()
		},
	})

	return lg, nil
}
