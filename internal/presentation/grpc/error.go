package grpc

import "errors"

var (
	ErrNilUserService = errors.New("user service is nil")
	ErrNilLogger      = errors.New("logger is nil")
)
