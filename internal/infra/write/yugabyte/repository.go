package repository

import (
	"context"
	domain "user-service/internal/domain"
	"user-service/internal/infra/write/yugabyte/mapper"
	"user-service/internal/infra/write/yugabyte/model"

	"github.com/jmoiron/sqlx"
)

type repo struct {
	db         *sqlx.DB
	translator DBErrorTranslator
}

// New constructs the Yugabyte-backed user repository.
func New(db *sqlx.DB, translator DBErrorTranslator) (domain.UserRepository, error) {
	if db == nil {
		return nil, ErrNilYugaByteDB
	}
	if translator == nil {
		return nil, ErrNilDBErrorTranslator
	}

	return &repo{db: db, translator: translator}, nil
}

func (r *repo) Create(ctx context.Context, params domain.CreateUserParams) (*domain.User, error) {
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
		&userRow.IsActive,
		&userRow.Status,
		&userRow.CreatedAt,
		&userRow.UpdatedAt,
	); err != nil {
		return nil, r.translator.TranslateCreateUserError(err)
	}

	return mapper.MapUserRowToDomain(userRow), nil
}

func (r *repo) GetByID(ctx context.Context, userID string) (*domain.User, error) {
	var userRow model.UserRow
	if err := r.db.QueryRowContext(ctx, getUserByID, userID).Scan(
		&userRow.ID,
		&userRow.Username,
		&userRow.FirstName,
		&userRow.LastName,
		&userRow.IsActive,
		&userRow.Status,
		&userRow.CreatedAt,
		&userRow.UpdatedAt,
	); err != nil {
		return nil, r.translator.TranslateFindUserError(err)
	}

	return mapper.MapUserRowToDomain(userRow), nil
}

func (r *repo) ExistsByUsername(ctx context.Context, username string) (bool, error) {
	var exists bool
	if err := r.db.QueryRowContext(ctx, existsByUsernameQuery, username).Scan(&exists); err != nil {
		return false, WrapFindUserError(err)
	}

	return exists, nil
}

func (r *repo) ActivateByID(ctx context.Context, userID string) (*domain.User, error) {
	var userRow model.UserRow
	if err := r.db.QueryRowContext(ctx, activateUserByID, userID).Scan(
		&userRow.ID,
		&userRow.Username,
		&userRow.FirstName,
		&userRow.LastName,
		&userRow.IsActive,
		&userRow.Status,
		&userRow.CreatedAt,
		&userRow.UpdatedAt,
	); err != nil {
		return nil, r.translator.TranslateFindUserError(err)
	}

	return mapper.MapUserRowToDomain(userRow), nil
}

func (r *repo) DeactivateByID(ctx context.Context, userID string) error {
	result, err := r.db.ExecContext(ctx, deactivateUserByID, userID)
	if err != nil {
		return r.translator.TranslateDeleteUserError(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return r.translator.TranslateDeleteUserError(err)
	}
	if rowsAffected == 0 {
		return domain.ErrUserNotFound
	}

	return nil
}

func (r *repo) DeleteByID(ctx context.Context, userID string) error {
	result, err := r.db.ExecContext(ctx, deleteUserByID, userID)
	if err != nil {
		return r.translator.TranslateDeleteUserError(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return r.translator.TranslateDeleteUserError(err)
	}
	if rowsAffected == 0 {
		return domain.ErrUserNotFound
	}

	return nil
}
