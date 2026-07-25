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
	GetUserPreviewByID(ctx context.Context, userID string) (*domain.User, error)
	GetUserPreviewByIDNoCache(ctx context.Context, userID string) (*domain.User, error)
	GetDetailedUserByUsername(ctx context.Context, username string) (*domain.User, error)
}

// UserRepository aliases the write-model persistence contract consumed by the
// application layer.
type UserRepository = domain.UserRepository

// UserReadRepository aliases the read-model persistence contract consumed by
// the application layer.
type UserReadRepository = domain.UserReadRepository

// FileURLClient resolves public URLs for file identifiers.
type FileURLClient interface {
	GetFileURL(ctx context.Context, fileID string) (string, error)
}

// DetailedUserPublisher emits best-effort detailed-user projection requests.
type DetailedUserPublisher interface {
	PublishDetailedUserRequested(ctx context.Context, user *domain.User) error
}

// Logger aliases the shared structured logger used by the application layer.
type Logger = logging.Logger
