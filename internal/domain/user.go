package user

import (
	"context"
	"time"
)

// User is the write-model entity owned by user-service.
type User struct {
	ID        string
	Username  string
	FirstName string
	LastName  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CreateUserParams contains the input required to create a new user profile.
type CreateUserParams struct {
	ID        string
	Username  string
	FirstName string
	LastName  string
}

// UserRepository persists the user-service write model.
type UserRepository interface {
	Create(ctx context.Context, params CreateUserParams) (*User, error)
	GetByID(ctx context.Context, userID string) (*User, error)
	ExistsByUsername(ctx context.Context, username string) (bool, error)
	DeleteByID(ctx context.Context, userID string) error
}

// UserReadRepository persists the user-service read-model projection.
type UserReadRepository interface {
	Upsert(ctx context.Context, user *User) error
	DeleteByID(ctx context.Context, userID string) error
}
