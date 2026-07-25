package grpc

import (
	domain "user-service/internal/domain"

	userv1 "github.com/ofm-microservices/ofm-common/proto/user/v1"
)

type userMapper struct{}

func newUserMapper() *userMapper {
	return &userMapper{}
}

func (m *userMapper) ToPreviewResponse(user *domain.User) *userv1.GetUserPreviewByIDResponse {
	if user == nil {
		return &userv1.GetUserPreviewByIDResponse{}
	}

	return &userv1.GetUserPreviewByIDResponse{
		User: &userv1.UserPreview{
			UserId:      user.ID,
			Username:    user.Username,
			DisplayName: domain.DisplayName(user.FirstName, user.LastName),
			AvatarId:    user.AvatarID,
			AvatarUrl:   user.AvatarURL,
		},
	}
}

func (m *userMapper) ToDetailedResponse(user *domain.User) *userv1.GetDetailedUserByUsernameResponse {
	if user == nil {
		return &userv1.GetDetailedUserByUsernameResponse{}
	}

	displayName := user.DisplayName
	if displayName == "" {
		displayName = domain.DisplayName(user.FirstName, user.LastName)
	}

	return &userv1.GetDetailedUserByUsernameResponse{
		User: &userv1.DetailedUser{
			UserId:      user.ID,
			Username:    user.Username,
			DisplayName: displayName,
			AvatarId:    user.AvatarID,
			AvatarUrl:   user.AvatarURL,
			About:       user.About,
		},
	}
}
