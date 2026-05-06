package mapper

import (
	domain "user-service/internal/domain"
	"user-service/internal/infra/read/redis/model"
)

// MapDomainUserToCache maps the domain user into its Redis projection model.
func MapDomainUserToCache(user *domain.User) model.UserCache {
	return model.UserCache{
		ID:        user.ID,
		Username:  user.Username,
		FirstName: user.FirstName,
		LastName:  user.LastName,
		IsActive:  user.IsActive,
		CreatedAt: user.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		UpdatedAt: user.UpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"),
	}
}
