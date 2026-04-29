package user

import "errors"

var (
	ErrInvalidUserID        = errors.New("invalid user id")
	ErrInvalidUsername      = errors.New("invalid username")
	ErrUserIDAlreadyTaken   = errors.New("user id is already taken")
	ErrUsernameAlreadyTaken = errors.New("username is already taken")
	ErrUserNotFound         = errors.New("user not found")
	ErrFailedToCreateUser   = errors.New("failed to create user")
	ErrFailedToFindUser     = errors.New("failed to find user")
	ErrFailedToDeleteUser   = errors.New("failed to delete user")
)
