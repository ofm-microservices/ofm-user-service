package repository

import (
	"errors"
	"fmt"
	domain "user-service/internal/domain"
)

var (
	ErrNilYugaByteDB        = errors.New("yugabyte db is nil")
	ErrNilDBErrorTranslator = errors.New("db error translator is nil")
	ErrNilLogger            = errors.New("logger is nil")
)

const domainWrapFormat = "%w: %v"

// WrapCreateUserError annotates user insert failures.
func WrapCreateUserError(err error) error {
	return fmt.Errorf(domainWrapFormat, domain.ErrFailedToCreateUser, err)
}

// WrapFindUserError annotates user lookup failures.
func WrapFindUserError(err error) error {
	return fmt.Errorf(domainWrapFormat, domain.ErrFailedToFindUser, err)
}

// WrapDeleteUserError annotates user delete failures.
func WrapDeleteUserError(err error) error {
	return fmt.Errorf(domainWrapFormat, domain.ErrFailedToDeleteUser, err)
}
