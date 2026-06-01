package service

import "errors"

var (
	ErrNilUserRepository        = errors.New("user repository is nil")
	ErrNilUserReadRepository    = errors.New("user read repository is nil")
	ErrNilFileURLClient         = errors.New("file url client is nil")
	ErrNilDetailedUserPublisher = errors.New("detailed user publisher is nil")
	ErrNilLogger                = errors.New("logger is nil")
)
