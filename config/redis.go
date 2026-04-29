package config

// RedisConfig defines the Redis read-model connection used by user-service.
type RedisConfig struct {
	Host     string `env:"REDIS_HOST,required"`
	Port     int    `env:"REDIS_PORT,required"`
	Password string `env:"REDIS_PASSWORD"`
	DB       int    `env:"REDIS_DB" envDefault:"0"`
}
