package mapper

import (
	"strings"

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
		AvatarID:  user.AvatarID,
		IsActive:  user.IsActive,
		CreatedAt: user.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		UpdatedAt: user.UpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"),
	}
}

// MapCacheToDomainUser maps the Redis preview cache into the user domain entity.
func MapCacheToDomainUser(cache model.UserCache) *domain.User {
	return &domain.User{
		ID:        cache.ID,
		Username:  cache.Username,
		FirstName: cache.FirstName,
		LastName:  cache.LastName,
		AvatarID:  cache.AvatarID,
		IsActive:  cache.IsActive,
	}
}

// DisplayName returns the human-readable display name used in preview payloads.
func DisplayName(firstName, lastName string) string {
	firstName = strings.TrimSpace(firstName)
	lastName = strings.TrimSpace(lastName)
	switch {
	case firstName == "" && lastName == "":
		return ""
	case firstName == "":
		return lastName
	case lastName == "":
		return firstName
	default:
		return firstName + " " + lastName
	}
}
