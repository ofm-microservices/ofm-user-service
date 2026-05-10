package service

import (
	"context"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	domain "user-service/internal/domain"
)

// UserService owns profile creation, deletion, and username availability
// checks within user-service.
type UserService interface {
	CreateUser(ctx context.Context, userID, username, firstName, lastName string) (*domain.User, error)
	ExistsByUsername(ctx context.Context, username string) (bool, error)
	ActivateUser(ctx context.Context, userID string) (*domain.User, error)
	DeactivateUser(ctx context.Context, userID string) error
	DeleteUser(ctx context.Context, userID string) error
}

// UserRepository aliases the write-model persistence contract consumed by the
// application layer.
type UserRepository = domain.UserRepository

// UserReadRepository aliases the read-model persistence contract consumed by
// the application layer.
type UserReadRepository = domain.UserReadRepository

// Logger aliases the shared structured logger used by the application layer.
type Logger = logging.Logger
