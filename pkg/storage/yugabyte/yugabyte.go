package db

import (
	"fmt"
	"time"
	"user-service/config"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

var connectDB = sqlx.Connect

// Open creates the user-service YugabyteDB connection pool.
func Open(cfg config.DBConfig) (*sqlx.DB, error) {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Name,
		cfg.SSLMode,
	)

	db, err := connectDB("pgx", dsn)
	if err != nil {
		return nil, WrapOpenDBError(err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(2 * time.Minute)

	return db, nil
}
