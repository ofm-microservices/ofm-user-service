package mapper

import (
	domain "user-service/internal/domain"
	"user-service/internal/infra/write/yugabyte/model"
)

// MapUserRowToDomain maps the Yugabyte row to the user domain entity.
func MapUserRowToDomain(userRow model.UserRow) *domain.User {
	return &domain.User{
		ID:        userRow.ID,
		Username:  userRow.Username,
		FirstName: userRow.FirstName,
		LastName:  userRow.LastName,
		AvatarID:  userRow.AvatarID,
		About:     userRow.About,
		IsActive:  userRow.IsActive,
		Status:    userRow.Status,
		CreatedAt: userRow.CreatedAt,
		UpdatedAt: userRow.UpdatedAt,
	}
}
