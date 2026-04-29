package nats

import (
	"errors"
	domain "user-service/internal/domain"
)

// FailureReasonResolver maps domain errors to stable saga-facing failure
// reasons.
type FailureReasonResolver interface {
	CreateUserFailureReason(err error) string
	DeleteUserFailureReason(err error) string
}

// DomainFailureReasonResolver is the default domain-error-to-string mapper for
// user saga results.
type DomainFailureReasonResolver struct{}

// NewDomainFailureReasonResolver constructs the default failure reason
// resolver.
func NewDomainFailureReasonResolver() FailureReasonResolver {
	return &DomainFailureReasonResolver{}
}

func (r *DomainFailureReasonResolver) CreateUserFailureReason(err error) string {
	switch {
	case errors.Is(err, domain.ErrInvalidUserID):
		return "invalid user_id"
	case errors.Is(err, domain.ErrUserIDAlreadyTaken):
		return "user_id already taken"
	case errors.Is(err, domain.ErrUsernameAlreadyTaken):
		return "username already taken"
	default:
		return "failed to create user"
	}
}

func (r *DomainFailureReasonResolver) DeleteUserFailureReason(err error) string {
	switch {
	case errors.Is(err, domain.ErrInvalidUserID):
		return "invalid user_id"
	case errors.Is(err, domain.ErrUserNotFound):
		return "user not found"
	default:
		return "failed to delete user"
	}
}
