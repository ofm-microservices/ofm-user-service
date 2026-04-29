package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	user "user-service/internal/domain"
)

func TestApplication(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Application Suite")
}

var _ = Describe("UserService", func() {
	var (
		ctrl     *gomock.Controller
		repo     *MockUserRepository
		readRepo *MockUserReadRepository
		logger   logging.Logger
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		repo = NewMockUserRepository(ctrl)
		readRepo = NewMockUserReadRepository(ctrl)

		var err error
		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("New", func() {
		It("validates nil collaborators", func() {
			svc, err := New(nil, readRepo, logger)
			Expect(svc).To(BeNil())
			Expect(err).To(MatchError(ErrNilUserRepository))

			svc, err = New(repo, nil, logger)
			Expect(svc).To(BeNil())
			Expect(err).To(MatchError(ErrNilUserReadRepository))

			svc, err = New(repo, readRepo, nil)
			Expect(svc).To(BeNil())
			Expect(err).To(MatchError(ErrNilLogger))
		})
	})

	Describe("CreateUser", func() {
		It("rejects an empty user id", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			result, err := svc.CreateUser(context.Background(), "", "alex", "Alex", "Doe")

			Expect(result).To(BeNil())
			Expect(err).To(MatchError(user.ErrInvalidUserID))
		})

		It("creates the write model and projects it to the read model", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			createdAt := time.Date(2026, time.April, 24, 10, 0, 0, 0, time.UTC)
			expectedUser := &user.User{
				ID:        "user-1",
				Username:  "alex",
				FirstName: "Alex",
				LastName:  "Doe",
				CreatedAt: createdAt,
				UpdatedAt: createdAt,
			}

			repo.EXPECT().
				Create(gomock.Any(), user.CreateUserParams{
					ID:        "user-1",
					Username:  "alex",
					FirstName: "Alex",
					LastName:  "Doe",
				}).
				Return(expectedUser, nil)

			readRepo.EXPECT().
				Upsert(gomock.Any(), expectedUser).
				Return(nil)

			result, err := svc.CreateUser(context.Background(), "user-1", "alex", "Alex", "Doe")

			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(expectedUser))
		})

		It("returns write-model repository failures", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			repo.EXPECT().
				Create(gomock.Any(), gomock.Any()).
				Return(nil, user.ErrUsernameAlreadyTaken)

			result, err := svc.CreateUser(context.Background(), "user-1", "alex", "Alex", "Doe")

			Expect(result).To(BeNil())
			Expect(err).To(MatchError(user.ErrUsernameAlreadyTaken))
		})

		It("returns read-model projection failures after creation", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			expectedUser := &user.User{
				ID:       "user-1",
				Username: "alex",
			}

			repo.EXPECT().
				Create(gomock.Any(), gomock.Any()).
				Return(expectedUser, nil)

			readRepo.EXPECT().
				Upsert(gomock.Any(), expectedUser).
				Return(errors.New("redis unavailable"))

			result, err := svc.CreateUser(context.Background(), "user-1", "alex", "Alex", "Doe")

			Expect(result).To(BeNil())
			Expect(err).To(MatchError("redis unavailable"))
		})
	})

	Describe("DeleteUser", func() {
		It("rejects an empty user id", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			err = svc.DeleteUser(context.Background(), "")

			Expect(err).To(MatchError(user.ErrInvalidUserID))
		})

		It("deletes the write model and read model", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			repo.EXPECT().DeleteByID(gomock.Any(), "user-1").Return(nil)
			readRepo.EXPECT().DeleteByID(gomock.Any(), "user-1").Return(nil)

			Expect(svc.DeleteUser(context.Background(), "user-1")).To(Succeed())
		})

		It("returns write-model delete failures", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			repo.EXPECT().DeleteByID(gomock.Any(), "user-1").Return(user.ErrUserNotFound)

			err = svc.DeleteUser(context.Background(), "user-1")

			Expect(err).To(MatchError(user.ErrUserNotFound))
		})

		It("returns read-model delete failures after removing the write model", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			repo.EXPECT().DeleteByID(gomock.Any(), "user-1").Return(nil)
			readRepo.EXPECT().DeleteByID(gomock.Any(), "user-1").Return(errors.New("redis unavailable"))

			err = svc.DeleteUser(context.Background(), "user-1")

			Expect(err).To(MatchError("redis unavailable"))
		})
	})

	Describe("ExistsByUsername", func() {
		It("rejects a blank username after trimming", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			exists, err := svc.ExistsByUsername(context.Background(), "   ")

			Expect(exists).To(BeFalse())
			Expect(err).To(MatchError(user.ErrInvalidUsername))
		})

		It("trims the username before delegating to the repository", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			repo.EXPECT().ExistsByUsername(gomock.Any(), "alex").Return(true, nil)

			exists, err := svc.ExistsByUsername(context.Background(), " alex ")

			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
		})

		It("returns repository lookup failures", func() {
			svc, err := New(repo, readRepo, logger)
			Expect(err).NotTo(HaveOccurred())

			repo.EXPECT().ExistsByUsername(gomock.Any(), "alex").Return(false, errors.New("lookup failed"))

			exists, err := svc.ExistsByUsername(context.Background(), "alex")

			Expect(exists).To(BeFalse())
			Expect(err).To(MatchError("lookup failed"))
		})
	})
})
