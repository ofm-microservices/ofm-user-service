package config

import "github.com/caarlos0/env/v11"

// Config groups the full user-service runtime configuration.
type Config struct {
	App     AppConfig
	DB      DBConfig
	GRPC    GRPCConfig
	Metrics MetricsConfig
	Tracing TracingConfig
	File    FileServiceConfig
	Redis   RedisConfig
	NATS    NATSConfig
}

// Load reads environment variables into Config and applies defaults.
func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, WrapParseEnvConfigError(err)
	}

	return cfg, nil
}
