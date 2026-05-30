package grpc

import (
	domain "user-service/internal/domain"
	"user-service/internal/infra/read/redis/mapper"

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
			DisplayName: mapper.DisplayName(user.FirstName, user.LastName),
			AvatarId:    user.AvatarID,
		},
	}
}
