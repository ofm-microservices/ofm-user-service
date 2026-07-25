package repository

import (
	"context"
	"time"
	domain "user-service/internal/domain"
	"user-service/internal/infra/write/yugabyte/mapper"
	"user-service/internal/infra/write/yugabyte/model"

	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
)

type repo struct {
	db         *sqlx.DB
	translator DBErrorTranslator
	log        logging.Logger
}

// New constructs the Yugabyte-backed user repository.
func New(db *sqlx.DB, translator DBErrorTranslator, log logging.Logger) (domain.UserRepository, error) {
	if db == nil {
		return nil, ErrNilYugaByteDB
	}
	if translator == nil {
		return nil, ErrNilDBErrorTranslator
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &repo{db: db, translator: translator, log: log.With(logging.String("module", "yugabyte-repository"))}, nil
}

func (r *repo) Create(ctx context.Context, params domain.CreateUserParams) (*domain.User, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveDB("yugabyte", "create", "users", status, time.Since(started)) }()

	var userRow model.UserRow
	if err := r.db.QueryRowContext(
		ctx,
		createUserQuery,
		params.ID,
		params.Username,
		params.FirstName,
		params.LastName,
	).Scan(
		&userRow.ID,
		&userRow.Username,
		&userRow.FirstName,
		&userRow.LastName,
		&userRow.AvatarID,
		&userRow.About,
		&userRow.IsActive,
		&userRow.Status,
		&userRow.CreatedAt,
		&userRow.UpdatedAt,
	); err != nil {
		status = "error"
		r.log.Error("create user failed",
			logging.Operation("db.user.create"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", params.ID),
			logging.Err(err),
		)
		return nil, r.translator.TranslateCreateUserError(err)
	}

	return mapper.MapUserRowToDomain(userRow), nil
}

func (r *repo) GetByID(ctx context.Context, userID string) (*domain.User, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveDB("yugabyte", "get_by_id", "users", status, time.Since(started)) }()

	var userRow model.UserRow
	if err := r.db.QueryRowContext(ctx, getUserByID, userID).Scan(
		&userRow.ID,
		&userRow.Username,
		&userRow.FirstName,
		&userRow.LastName,
		&userRow.AvatarID,
		&userRow.About,
		&userRow.IsActive,
		&userRow.Status,
		&userRow.CreatedAt,
		&userRow.UpdatedAt,
	); err != nil {
		status = "error"
		r.log.Error("get user failed",
			logging.Operation("db.user.get_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", userID),
			logging.Err(err),
		)
		return nil, r.translator.TranslateFindUserError(err)
	}

	return mapper.MapUserRowToDomain(userRow), nil
}

func (r *repo) GetByUsername(ctx context.Context, username string) (*domain.User, error) {
	started := time.Now()
	status := "success"
	defer func() {
		metrics.Global().ObserveDB("yugabyte", "get_by_username", "users", status, time.Since(started))
	}()

	var userRow model.UserRow
	if err := r.db.QueryRowContext(ctx, getUserByUsername, username).Scan(
		&userRow.ID,
		&userRow.Username,
		&userRow.FirstName,
		&userRow.LastName,
		&userRow.AvatarID,
		&userRow.About,
		&userRow.IsActive,
		&userRow.Status,
		&userRow.CreatedAt,
		&userRow.UpdatedAt,
	); err != nil {
		status = "error"
		r.log.Error("get user by username failed",
			logging.Operation("db.user.get_by_username"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("username", username),
			logging.Err(err),
		)
		return nil, r.translator.TranslateFindUserError(err)
	}

	return mapper.MapUserRowToDomain(userRow), nil
}

func (r *repo) ExistsByUsername(ctx context.Context, username string) (bool, error) {
	started := time.Now()
	status := "success"
	defer func() {
		metrics.Global().ObserveDB("yugabyte", "exists_by_username", "users", status, time.Since(started))
	}()

	var exists bool
	if err := r.db.QueryRowContext(ctx, existsByUsernameQuery, username).Scan(&exists); err != nil {
		status = "error"
		r.log.Error("exists by username failed",
			logging.Operation("db.user.exists_by_username"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("username", username),
			logging.Err(err),
		)
		return false, WrapFindUserError(err)
	}

	return exists, nil
}

func (r *repo) ActivateByID(ctx context.Context, userID string) (*domain.User, error) {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveDB("yugabyte", "activate_by_id", "users", status, time.Since(started)) }()

	var userRow model.UserRow
	if err := r.db.QueryRowContext(ctx, activateUserByID, userID).Scan(
		&userRow.ID,
		&userRow.Username,
		&userRow.FirstName,
		&userRow.LastName,
		&userRow.AvatarID,
		&userRow.About,
		&userRow.IsActive,
		&userRow.Status,
		&userRow.CreatedAt,
		&userRow.UpdatedAt,
	); err != nil {
		status = "error"
		r.log.Error("activate user failed",
			logging.Operation("db.user.activate_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", userID),
			logging.Err(err),
		)
		return nil, r.translator.TranslateFindUserError(err)
	}

	return mapper.MapUserRowToDomain(userRow), nil
}

func (r *repo) DeactivateByID(ctx context.Context, userID string) error {
	started := time.Now()
	status := "success"
	defer func() {
		metrics.Global().ObserveDB("yugabyte", "deactivate_by_id", "users", status, time.Since(started))
	}()

	result, err := r.db.ExecContext(ctx, deactivateUserByID, userID)
	if err != nil {
		status = "error"
		r.log.Error("deactivate user failed",
			logging.Operation("db.user.deactivate_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", userID),
			logging.Err(err),
		)
		return r.translator.TranslateDeleteUserError(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		status = "error"
		r.log.Error("deactivate user rows affected failed",
			logging.Operation("db.user.deactivate_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", userID),
			logging.Err(err),
		)
		return r.translator.TranslateDeleteUserError(err)
	}
	if rowsAffected == 0 {
		status = "error"
		return domain.ErrUserNotFound
	}

	return nil
}

func (r *repo) DeleteByID(ctx context.Context, userID string) error {
	started := time.Now()
	status := "success"
	defer func() { metrics.Global().ObserveDB("yugabyte", "delete_by_id", "users", status, time.Since(started)) }()

	result, err := r.db.ExecContext(ctx, deleteUserByID, userID)
	if err != nil {
		status = "error"
		r.log.Error("delete user failed",
			logging.Operation("db.user.delete_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", userID),
			logging.Err(err),
		)
		return r.translator.TranslateDeleteUserError(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		status = "error"
		r.log.Error("delete user rows affected failed",
			logging.Operation("db.user.delete_by_id"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", userID),
			logging.Err(err),
		)
		return r.translator.TranslateDeleteUserError(err)
	}
	if rowsAffected == 0 {
		status = "error"
		return domain.ErrUserNotFound
	}

	return nil
}
