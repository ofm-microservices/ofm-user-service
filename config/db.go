package config

import "time"

// DBConfig defines the YugabyteDB connection and migration settings owned by
// user-service.
type DBConfig struct {
	Host            string        `env:"DB_HOST,required"`
	Port            int           `env:"DB_PORT,required"`
	User            string        `env:"DB_USER,required"`
	Password        string        `env:"DB_PASSWORD,required"`
	Name            string        `env:"DB_NAME,required"`
	SSLMode         string        `env:"DB_SSLMODE" envDefault:"disable"`
	MaxOpenConns    int           `env:"DB_MAX_OPEN_CONNS" envDefault:"20"`
	MaxIdleConns    int           `env:"DB_MAX_IDLE_CONNS" envDefault:"10"`
	ConnMaxLifetime time.Duration `env:"DB_CONN_MAX_LIFETIME" envDefault:"5m"`
	MigrationsPath  string        `env:"MIGRATIONS_PATH" envDefault:"file://migration/yugabyte"`
	MigrationsTable string        `env:"MIGRATIONS_TABLE" envDefault:"schema_migrations_user_service"`
}
