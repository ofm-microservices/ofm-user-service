package appfx

import (
	user "user-service/internal/domain"
	readrepo "user-service/internal/infra/read/redis"
	writerepo "user-service/internal/infra/write/yugabyte"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
)

// RepoModule wires write- and read-model repositories into the FX graph.
var RepoModule = fx.Options(
	fx.Provide(
		writerepo.NewPgErrorTranslator,
		ProvideWriteRepo,
		ProvideReadRepo,
	),
)

// ProvideWriteRepo constructs the Yugabyte-backed user repository.
func ProvideWriteRepo(dbx *sqlx.DB, translator writerepo.DBErrorTranslator) (user.UserRepository, error) {
	return writerepo.New(dbx, translator)
}

// ProvideReadRepo constructs the Redis-backed user read repository.
func ProvideReadRepo(rdb *redis.Client) (user.UserReadRepository, error) {
	return readrepo.New(rdb)
}
