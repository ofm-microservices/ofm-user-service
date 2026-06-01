package user

import (
	"context"
	"time"
)

const (
	// StatusPendingRegistration marks a profile created by the saga before the
	// user verifies email and completes registration.
	StatusPendingRegistration = "pending_registration"
	// StatusActive marks a profile that completed registration successfully.
	StatusActive = "active"
	// StatusRegistrationFailed marks a profile compensated by the registration
	// saga after a failed registration.
	StatusRegistrationFailed = "registration_failed"
)

// User is the write-model entity owned by user-service.
type User struct {
	ID          string
	Username    string
	DisplayName string
	FirstName   string
	LastName    string
	AvatarID    string
	AvatarURL   string
	About       string
	IsActive    bool
	Status      string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CreateUserParams contains the input required to create a new user profile.
type CreateUserParams struct {
	ID        string
	Username  string
	FirstName string
	LastName  string
	AvatarID  string
}

// UserRepository persists the user-service write model.
type UserRepository interface {
	Create(ctx context.Context, params CreateUserParams) (*User, error)
	GetByID(ctx context.Context, userID string) (*User, error)
	GetByUsername(ctx context.Context, username string) (*User, error)
	ExistsByUsername(ctx context.Context, username string) (bool, error)
	ActivateByID(ctx context.Context, userID string) (*User, error)
	DeactivateByID(ctx context.Context, userID string) error
	DeleteByID(ctx context.Context, userID string) error
}

// UserReadRepository persists the user-service read-model projection.
type UserReadRepository interface {
	Upsert(ctx context.Context, user *User) error
	GetByID(ctx context.Context, userID string) (*User, error)
	DeleteByID(ctx context.Context, userID string) error
	UpsertByUsername(ctx context.Context, user *User) error
	GetByUsername(ctx context.Context, username string) (*User, error)
	DeleteByUsername(ctx context.Context, username string) error
}
