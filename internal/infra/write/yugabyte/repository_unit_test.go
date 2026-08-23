package repository

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	user "user-service/internal/domain"
)

type fakeTranslator struct {
	createErr error
	findErr   error
	deleteErr error
}

func (t fakeTranslator) TranslateCreateUserError(error) error {
	return t.createErr
}

func (t fakeTranslator) TranslateFindUserError(error) error {
	return t.findErr
}

func (t fakeTranslator) TranslateDeleteUserError(error) error {
	return t.deleteErr
}

var _ = Describe("repository unit", func() {
	var (
		db      *sql.DB
		dbx     *sqlx.DB
		mock    sqlmock.Sqlmock
		repoAny user.UserRepository
		logger  logging.Logger
	)

	BeforeEach(func() {
		var err error
		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
		db, mock, err = sqlmock.New()
		Expect(err).NotTo(HaveOccurred())
		dbx = sqlx.NewDb(db, "sqlmock")
		repoAny, err = New(dbx, fakeTranslator{
			createErr: user.ErrFailedToCreateUser,
			findErr:   user.ErrFailedToFindUser,
			deleteErr: user.ErrFailedToDeleteUser,
		}, logger)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(mock.ExpectationsWereMet()).To(Succeed())
		mock.ExpectClose()
		Expect(db.Close()).To(Succeed())
	})

	It("validates constructor dependencies without a database", func() {
		repo, err := New(nil, fakeTranslator{}, logger)
		Expect(repo).To(BeNil())
		Expect(err).To(MatchError(ErrNilYugaByteDB))

		repo, err = New(dbx, nil, logger)
		Expect(repo).To(BeNil())
		Expect(err).To(MatchError(ErrNilDBErrorTranslator))
	})

	It("creates, reads, and activates users from returned rows", func() {
		now := time.Now().UTC()
		rows := sqlmock.NewRows([]string{"user_id", "username", "first_name", "last_name", "avatar_id", "about", "is_active", "status", "created_at", "updated_at"}).
			AddRow("user-1", "alex", "Alex", "Doe", "", "", false, user.StatusPendingRegistration, now, now)
		mock.ExpectQuery(regexp.QuoteMeta(createUserQuery)).
			WithArgs("user-1", "alex", "Alex", "Doe", "").
			WillReturnRows(rows)

		created, err := repoAny.Create(context.Background(), user.CreateUserParams{
			ID:        "user-1",
			Username:  "alex",
			FirstName: "Alex",
			LastName:  "Doe",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(created.ID).To(Equal("user-1"))
		Expect(created.Status).To(Equal(user.StatusPendingRegistration))

		readRows := sqlmock.NewRows([]string{"user_id", "username", "first_name", "last_name", "avatar_id", "about", "is_active", "status", "created_at", "updated_at"}).
			AddRow("user-1", "alex", "Alex", "Doe", "", "", false, user.StatusPendingRegistration, now, now)
		mock.ExpectQuery(regexp.QuoteMeta(getUserByID)).
			WithArgs("user-1").
			WillReturnRows(readRows)

		loaded, err := repoAny.GetByID(context.Background(), "user-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.Username).To(Equal("alex"))

		activeRows := sqlmock.NewRows([]string{"user_id", "username", "first_name", "last_name", "avatar_id", "about", "is_active", "status", "created_at", "updated_at"}).
			AddRow("user-1", "alex", "Alex", "Doe", "", "", true, user.StatusActive, now, now)
		mock.ExpectQuery(regexp.QuoteMeta(activateUserByID)).
			WithArgs("user-1").
			WillReturnRows(activeRows)

		activated, err := repoAny.ActivateByID(context.Background(), "user-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(activated.IsActive).To(BeTrue())
		Expect(activated.Status).To(Equal(user.StatusActive))
	})

	It("translates create, read, and activate query failures", func() {
		mock.ExpectQuery(regexp.QuoteMeta(createUserQuery)).
			WithArgs("user-1", "alex", "Alex", "Doe", "").
			WillReturnError(errors.New("insert failed"))

		created, err := repoAny.Create(context.Background(), user.CreateUserParams{
			ID:        "user-1",
			Username:  "alex",
			FirstName: "Alex",
			LastName:  "Doe",
		})
		Expect(created).To(BeNil())
		Expect(err).To(MatchError(user.ErrFailedToCreateUser))

		mock.ExpectQuery(regexp.QuoteMeta(getUserByID)).
			WithArgs("user-1").
			WillReturnError(sql.ErrNoRows)
		loaded, err := repoAny.GetByID(context.Background(), "user-1")
		Expect(loaded).To(BeNil())
		Expect(err).To(MatchError(user.ErrFailedToFindUser))

		mock.ExpectQuery(regexp.QuoteMeta(activateUserByID)).
			WithArgs("user-1").
			WillReturnError(sql.ErrNoRows)
		activated, err := repoAny.ActivateByID(context.Background(), "user-1")
		Expect(activated).To(BeNil())
		Expect(err).To(MatchError(user.ErrFailedToFindUser))
	})

	It("reports username existence and wraps lookup failures", func() {
		mock.ExpectQuery(regexp.QuoteMeta(existsByUsernameQuery)).
			WithArgs("alex").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

		exists, err := repoAny.ExistsByUsername(context.Background(), "alex")
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeTrue())

		mock.ExpectQuery(regexp.QuoteMeta(existsByUsernameQuery)).
			WithArgs("broken").
			WillReturnError(errors.New("select failed"))

		exists, err = repoAny.ExistsByUsername(context.Background(), "broken")
		Expect(exists).To(BeFalse())
		Expect(errors.Is(err, user.ErrFailedToFindUser)).To(BeTrue())
	})

	It("deactivates and deletes users with rows affected checks", func() {
		mock.ExpectExec(regexp.QuoteMeta(deactivateUserByID)).
			WithArgs("user-1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		Expect(repoAny.DeactivateByID(context.Background(), "user-1")).To(Succeed())

		mock.ExpectExec(regexp.QuoteMeta(deleteUserByID)).
			WithArgs("user-1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		Expect(repoAny.DeleteByID(context.Background(), "user-1")).To(Succeed())
	})

	It("translates exec and rows-affected failures", func() {
		mock.ExpectExec(regexp.QuoteMeta(deactivateUserByID)).
			WithArgs("user-1").
			WillReturnError(errors.New("update failed"))
		Expect(repoAny.DeactivateByID(context.Background(), "user-1")).To(MatchError(user.ErrFailedToDeleteUser))

		mock.ExpectExec(regexp.QuoteMeta(deactivateUserByID)).
			WithArgs("user-1").
			WillReturnResult(sqlmock.NewErrorResult(errors.New("rows failed")))
		Expect(repoAny.DeactivateByID(context.Background(), "user-1")).To(MatchError(user.ErrFailedToDeleteUser))

		mock.ExpectExec(regexp.QuoteMeta(deleteUserByID)).
			WithArgs("user-1").
			WillReturnError(errors.New("delete failed"))
		Expect(repoAny.DeleteByID(context.Background(), "user-1")).To(MatchError(user.ErrFailedToDeleteUser))

		mock.ExpectExec(regexp.QuoteMeta(deleteUserByID)).
			WithArgs("user-1").
			WillReturnResult(sqlmock.NewErrorResult(errors.New("rows failed")))
		Expect(repoAny.DeleteByID(context.Background(), "user-1")).To(MatchError(user.ErrFailedToDeleteUser))
	})

	It("returns user not found when no rows are affected", func() {
		mock.ExpectExec(regexp.QuoteMeta(deactivateUserByID)).
			WithArgs("missing").
			WillReturnResult(sqlmock.NewResult(0, 0))
		Expect(repoAny.DeactivateByID(context.Background(), "missing")).To(MatchError(user.ErrUserNotFound))

		mock.ExpectExec(regexp.QuoteMeta(deleteUserByID)).
			WithArgs("missing").
			WillReturnResult(sqlmock.NewResult(0, 0))
		Expect(repoAny.DeleteByID(context.Background(), "missing")).To(MatchError(user.ErrUserNotFound))
	})
})
