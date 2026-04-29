package service

import "errors"

var (
	ErrNilUserRepository     = errors.New("user repository is nil")
	ErrNilUserReadRepository = errors.New("user read repository is nil")
	ErrNilLogger             = errors.New("logger is nil")
)
