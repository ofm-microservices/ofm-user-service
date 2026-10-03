package kafka

import (
	"errors"
	user "user-service/internal/domain"
)

// DomainFailureReasonResolver maps user domain errors to stable saga reasons.
type DomainFailureReasonResolver struct{}

// NewDomainFailureReasonResolver constructs the Kafka saga failure resolver.
func NewDomainFailureReasonResolver() FailureReasonResolver { return &DomainFailureReasonResolver{} }

// CreateUserFailureReason maps create-user failures for saga consumers.
func (r *DomainFailureReasonResolver) CreateUserFailureReason(err error) string {
	switch {
	case errors.Is(err, user.ErrInvalidUserID):
		return "invalid user_id"
	case errors.Is(err, user.ErrUserIDAlreadyTaken):
		return "user_id already taken"
	case errors.Is(err, user.ErrUsernameAlreadyTaken):
		return "username already taken"
	default:
		return "failed to create user"
	}
}

// DeleteUserFailureReason maps delete-user failures for saga consumers.
func (r *DomainFailureReasonResolver) DeleteUserFailureReason(err error) string {
	switch {
	case errors.Is(err, user.ErrInvalidUserID):
		return "invalid user_id"
	case errors.Is(err, user.ErrUserNotFound):
		return "user not found"
	default:
		return "failed to delete user"
	}
}
