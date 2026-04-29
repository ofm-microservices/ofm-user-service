package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	domain "user-service/internal/domain"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
)

const (
	UsersPrimaryKeyConstraint = "users_pkey"
	UsersUsernameConstraint   = "users_username_key"
)

// PgErrorTranslator converts pgx/Yugabyte errors into domain-aware repository
// errors.
type PgErrorTranslator struct{}

// NewPgErrorTranslator constructs the default Yugabyte error translator.
func NewPgErrorTranslator() DBErrorTranslator {
	return &PgErrorTranslator{}
}

func (t *PgErrorTranslator) TranslateCreateUserError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgerrcode.UniqueViolation:
			switch pgErr.ConstraintName {
			case UsersPrimaryKeyConstraint:
				return WrapDomainError(domain.ErrUserIDAlreadyTaken, err)
			case UsersUsernameConstraint:
				return WrapDomainError(domain.ErrUsernameAlreadyTaken, err)
			}
		case pgerrcode.InvalidTextRepresentation:
			return WrapDomainError(domain.ErrInvalidUserID, err)
		}
	}

	errMsg := err.Error()
	switch {
	case strings.Contains(errMsg, "SQLSTATE "+pgerrcode.UniqueViolation):
		switch {
		case strings.Contains(errMsg, UsersPrimaryKeyConstraint):
			return WrapDomainError(domain.ErrUserIDAlreadyTaken, err)
		case strings.Contains(errMsg, UsersUsernameConstraint):
			return WrapDomainError(domain.ErrUsernameAlreadyTaken, err)
		}
	case strings.Contains(errMsg, "SQLSTATE "+pgerrcode.InvalidTextRepresentation):
		return WrapDomainError(domain.ErrInvalidUserID, err)
	}

	return WrapCreateUserError(err)
}

func (t *PgErrorTranslator) TranslateFindUserError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return WrapDomainError(domain.ErrUserNotFound, err)
	}

	return WrapFindUserError(err)
}

func (t *PgErrorTranslator) TranslateDeleteUserError(err error) error {
	return WrapDeleteUserError(err)
}

// WrapDomainError preserves the domain error while attaching the original
// database cause for logging and debugging.
func WrapDomainError(domainErr, err error) error {
	return fmt.Errorf("%w: %v", domainErr, err)
}
