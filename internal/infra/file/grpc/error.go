package grpc

import "errors"

var (
	ErrEmptyAddress = errors.New("address is empty")
	ErrNilLogger    = errors.New("logger is nil")
)
