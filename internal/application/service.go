package service

import (
	"context"
	"errors"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"strings"
	domain "user-service/internal/domain"
)

type userService struct {
	repo     UserRepository
	readRepo UserReadRepository
	files    FileURLClient
	pub      DetailedUserPublisher
	log      Logger
}

// New constructs the user application service.
func New(repo UserRepository, readRepo UserReadRepository, log Logger) (UserService, error) {
	if repo == nil {
		return nil, ErrNilUserRepository
	}
	if readRepo == nil {
		return nil, ErrNilUserReadRepository
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &userService{
		repo:     repo,
		readRepo: readRepo,
		files:    noopFileURLClient{},
		pub:      noopDetailedUserPublisher{},
		log:      log.With(logging.String("module", "application")),
	}, nil
}

// NewDetailed constructs the user application service with outbound
// collaborators required by the detailed public user lookup flow.
func NewDetailed(repo UserRepository, readRepo UserReadRepository, files FileURLClient, pub DetailedUserPublisher, log Logger) (UserService, error) {
	if repo == nil {
		return nil, ErrNilUserRepository
	}
	if readRepo == nil {
		return nil, ErrNilUserReadRepository
	}
	if files == nil {
		return nil, ErrNilFileURLClient
	}
	if pub == nil {
		return nil, ErrNilDetailedUserPublisher
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &userService{
		repo:     repo,
		readRepo: readRepo,
		files:    files,
		pub:      pub,
		log:      log.With(logging.String("module", "application")),
	}, nil
}

func (s *userService) CreateUser(ctx context.Context, userID, username, firstName, lastName string) (*domain.User, error) {
	s.log.Info("create user command received", logging.String("user_id", userID), logging.String("username", username))

	if userID == "" {
		s.log.Error("invalid create user command", logging.String("reason", "empty user_id"))
		return nil, domain.ErrInvalidUserID
	}

	user, err := s.repo.Create(ctx, domain.CreateUserParams{
		ID:        userID,
		Username:  username,
		FirstName: firstName,
		LastName:  lastName,
		AvatarID:  "",
	})
	if err != nil {
		s.log.Error("failed to create user", logging.String("user_id", userID), logging.Err(err))
		return nil, err
	}

	if err := s.readRepo.Upsert(ctx, user); err != nil {
		s.log.Error("failed to upsert user read model", logging.String("user_id", user.ID), logging.Err(err))
		return nil, err
	}

	s.log.Info("user created", logging.String("user_id", user.ID), logging.String("username", user.Username))
	return user, nil
}

func (s *userService) DeleteUser(ctx context.Context, userID string) error {
	s.log.Info("delete user command received", logging.String("user_id", userID))

	if userID == "" {
		s.log.Error("invalid delete user command", logging.String("reason", "empty user_id"))
		return domain.ErrInvalidUserID
	}

	if err := s.repo.DeleteByID(ctx, userID); err != nil {
		s.log.Error("failed to delete user", logging.String("user_id", userID), logging.Err(err))
		return err
	}

	if err := s.readRepo.DeleteByID(ctx, userID); err != nil {
		s.log.Error("failed to delete user read model", logging.String("user_id", userID), logging.Err(err))
		return err
	}

	s.log.Info("user deleted", logging.String("user_id", userID))
	return nil
}

func (s *userService) ActivateUser(ctx context.Context, userID string) (*domain.User, error) {
	s.log.Info("activate user command received", logging.String("user_id", userID))

	if userID == "" {
		s.log.Error("invalid activate user command", logging.String("reason", "empty user_id"))
		return nil, domain.ErrInvalidUserID
	}

	user, err := s.repo.ActivateByID(ctx, userID)
	if err != nil {
		s.log.Error("failed to activate user", logging.String("user_id", userID), logging.Err(err))
		return nil, err
	}

	if err := s.readRepo.Upsert(ctx, user); err != nil {
		s.log.Error("failed to upsert activated user read model", logging.String("user_id", user.ID), logging.Err(err))
		return nil, err
	}

	s.log.Info("user activated", logging.String("user_id", user.ID))
	return user, nil
}

func (s *userService) DeactivateUser(ctx context.Context, userID string) error {
	s.log.Info("deactivate user command received", logging.String("user_id", userID))

	if userID == "" {
		s.log.Error("invalid deactivate user command", logging.String("reason", "empty user_id"))
		return domain.ErrInvalidUserID
	}

	if err := s.repo.DeactivateByID(ctx, userID); err != nil {
		s.log.Error("failed to deactivate user", logging.String("user_id", userID), logging.Err(err))
		return err
	}

	if err := s.readRepo.DeleteByID(ctx, userID); err != nil {
		s.log.Error("failed to delete deactivated user read model", logging.String("user_id", userID), logging.Err(err))
		return err
	}

	s.log.Info("user deactivated", logging.String("user_id", userID))
	return nil
}

func (s *userService) ExistsByUsername(ctx context.Context, username string) (bool, error) {
	if strings.TrimSpace(username) == "" {
		return false, domain.ErrInvalidUsername
	}

	return s.repo.ExistsByUsername(ctx, strings.TrimSpace(username))
}

func (s *userService) GetUserPreviewByID(ctx context.Context, userID string) (*domain.User, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, domain.ErrInvalidUserID
	}

	if cached, err := s.readRepo.GetByID(ctx, userID); err == nil && cached != nil {
		return cached, nil
	} else if err != nil && !errors.Is(err, domain.ErrUserNotFound) {
		s.log.Error("failed to get user preview from cache", logging.String("user_id", userID), logging.Err(err))
	}

	user, err := s.repo.GetByID(ctx, userID)
	if err != nil {
		s.log.Error("failed to get user preview from database", logging.String("user_id", userID), logging.Err(err))
		return nil, err
	}

	if err := s.readRepo.Upsert(ctx, user); err != nil {
		s.log.Error("failed to upsert user preview read model", logging.String("user_id", userID), logging.Err(err))
		return nil, err
	}

	return user, nil
}

func (s *userService) GetDetailedUserByUsername(ctx context.Context, username string) (*domain.User, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, domain.ErrInvalidUsername
	}

	if cached, err := s.readRepo.GetByUsername(ctx, username); err == nil && cached != nil {
		return cached, nil
	} else if err != nil && !errors.Is(err, domain.ErrUserNotFound) {
		s.log.Error("failed to get detailed user from cache", logging.String("username", username), logging.Err(err))
	}

	user, err := s.repo.GetByUsername(ctx, username)
	if err != nil {
		s.log.Error("failed to get detailed user from database", logging.String("username", username), logging.Err(err))
		return nil, err
	}

	if avatarID := strings.TrimSpace(user.AvatarID); avatarID != "" {
		if avatarURL, err := s.files.GetFileURL(ctx, avatarID); err == nil {
			user.AvatarURL = avatarURL
		} else {
			s.log.Error("failed to resolve detailed user avatar url",
				logging.String("username", username),
				logging.String("avatar_id", avatarID),
				logging.Err(err),
			)
		}
	}
	if user.DisplayName == "" {
		user.DisplayName = domain.DisplayName(user.FirstName, user.LastName)
	}

	if err := s.readRepo.UpsertByUsername(ctx, user); err != nil {
		s.log.Error("failed to upsert detailed user read model",
			logging.String("username", username),
			logging.Err(err),
		)
		return nil, err
	}

	if err := s.pub.PublishDetailedUserRequested(ctx, user); err != nil {
		s.log.Error("failed to publish detailed user projection request",
			logging.String("username", username),
			logging.Err(err),
		)
	}

	return user, nil
}

type noopFileURLClient struct{}

func (noopFileURLClient) GetFileURL(context.Context, string) (string, error) { return "", nil }

type noopDetailedUserPublisher struct{}

func (noopDetailedUserPublisher) PublishDetailedUserRequested(context.Context, *domain.User) error {
	return nil
}
