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

// MapDomainUserToDetailedCache maps the domain user into the detailed Redis
// public user projection.
func MapDomainUserToDetailedCache(user *domain.User) model.UserDetailedCache {
	displayName := strings.TrimSpace(user.DisplayName)
	if displayName == "" {
		displayName = DisplayName(user.FirstName, user.LastName)
	}
	return model.UserDetailedCache{
		ID:          user.ID,
		Username:    user.Username,
		DisplayName: displayName,
		AvatarID:    user.AvatarID,
		AvatarURL:   user.AvatarURL,
		About:       user.About,
		CreatedAt:   user.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"),
		UpdatedAt:   user.UpdatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z"),
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

// MapDetailedCacheToDomainUser maps the detailed Redis cache into the domain
// user entity.
func MapDetailedCacheToDomainUser(cache model.UserDetailedCache) *domain.User {
	return &domain.User{
		ID:          cache.ID,
		Username:    cache.Username,
		DisplayName: cache.DisplayName,
		AvatarID:    cache.AvatarID,
		AvatarURL:   cache.AvatarURL,
		About:       cache.About,
		IsActive:    true,
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
