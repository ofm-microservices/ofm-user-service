package appfx

import (
	"context"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	"user-service/config"
	rdb "user-service/pkg/storage/redis"
	ydb "user-service/pkg/storage/yugabyte"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
)

// StorageModule wires the user-service database, Redis read-model store, and
// migrations into the FX graph.
var StorageModule = fx.Options(
	fx.Invoke(InvokeRunMigrations),
	fx.Provide(
		ProvideYugaByteDB,
		ProvideRedisClient,
	),
)

var runMigrations = ydb.RunMigrations
var openYugaByteDB = ydb.Open
var openRedisClient = rdb.Open

// InvokeRunMigrations applies the user-service write-model migrations.
func InvokeRunMigrations(cfg *config.Config, lg logging.Logger) error {
	if err := runMigrations(cfg.DB); err != nil {
		lg.Error("run migrations failed", logging.Err(err))
		return err
	}

	lg.Info("migrations applied")
	return nil
}

// ProvideYugaByteDB opens the YugabyteDB connection owned by user-service.
func ProvideYugaByteDB(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (*sqlx.DB, error) {
	dbx, err := openYugaByteDB(cfg.DB)
	if err != nil {
		lg.Error("open database failed", logging.Err(err))
		return nil, err
	}

	lg.Info("database connected")

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			dbx.Close()
			return nil
		},
	})

	return dbx, nil
}

// ProvideRedisClient opens the Redis client used for the user read model.
func ProvideRedisClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (*redis.Client, error) {
	client, err := openRedisClient(context.Background(), cfg.Redis)
	if err != nil {
		lg.Error("open redis failed", logging.Err(err))
		return nil, err
	}

	lg.Info("redis connected")

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return client.Close()
		},
	})

	return client, nil
}
