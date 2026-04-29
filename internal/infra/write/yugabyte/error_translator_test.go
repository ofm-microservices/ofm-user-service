package repository

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	user "user-service/internal/domain"
)

func TestRepository(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Yugabyte Repository Suite")
}

var _ = Describe("PgErrorTranslator", func() {
	var translator DBErrorTranslator

	BeforeEach(func() {
		translator = NewPgErrorTranslator()
	})

	Describe("TranslateCreateUserError", func() {
		It("maps the primary-key conflict to the domain error", func() {
			err := translator.TranslateCreateUserError(&pgconn.PgError{
				Code:           pgerrcode.UniqueViolation,
				ConstraintName: UsersPrimaryKeyConstraint,
				Message:        "duplicate key",
			})

			Expect(errors.Is(err, user.ErrUserIDAlreadyTaken)).To(BeTrue())
		})

		It("maps the username conflict to the domain error", func() {
			err := translator.TranslateCreateUserError(&pgconn.PgError{
				Code:           pgerrcode.UniqueViolation,
				ConstraintName: UsersUsernameConstraint,
				Message:        "duplicate key",
			})

			Expect(errors.Is(err, user.ErrUsernameAlreadyTaken)).To(BeTrue())
		})

		It("maps invalid text representations to invalid user ids", func() {
			err := translator.TranslateCreateUserError(&pgconn.PgError{
				Code:    pgerrcode.InvalidTextRepresentation,
				Message: "invalid uuid",
			})

			Expect(errors.Is(err, user.ErrInvalidUserID)).To(BeTrue())
		})

		It("wraps unknown create failures", func() {
			err := translator.TranslateCreateUserError(errors.New("boom"))

			Expect(errors.Is(err, user.ErrFailedToCreateUser)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("boom"))
		})
	})

	Describe("TranslateFindUserError", func() {
		It("maps sql no rows to user not found", func() {
			err := translator.TranslateFindUserError(sql.ErrNoRows)

			Expect(errors.Is(err, user.ErrUserNotFound)).To(BeTrue())
		})

		It("wraps unknown find failures", func() {
			err := translator.TranslateFindUserError(errors.New("boom"))

			Expect(errors.Is(err, user.ErrFailedToFindUser)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("boom"))
		})
	})

	Describe("TranslateDeleteUserError", func() {
		It("wraps delete failures", func() {
			err := translator.TranslateDeleteUserError(errors.New("boom"))

			Expect(errors.Is(err, user.ErrFailedToDeleteUser)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("boom"))
		})
	})
})

var _ = Describe("WrapDomainError", func() {
	It("preserves the domain error and the original cause", func() {
		err := WrapDomainError(user.ErrUserNotFound, errors.New("sql no rows"))

		Expect(errors.Is(err, user.ErrUserNotFound)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("sql no rows"))
	})
})
