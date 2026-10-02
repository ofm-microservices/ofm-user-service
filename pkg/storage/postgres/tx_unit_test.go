package db

import (
	"errors"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("WithTx unit", func() {
	var (
		sqlDB sqlmock.Sqlmock
		dbx   *sqlx.DB
		rawDB interface{ Close() error }
	)

	BeforeEach(func() {
		db, mock, err := sqlmock.New()
		Expect(err).NotTo(HaveOccurred())
		sqlDB = mock
		dbx = sqlx.NewDb(db, "sqlmock")
		rawDB = db
	})

	AfterEach(func() {
		Expect(sqlDB.ExpectationsWereMet()).To(Succeed())
		sqlDB.ExpectClose()
		Expect(rawDB.Close()).To(Succeed())
	})

	It("commits when the callback succeeds", func() {
		sqlDB.ExpectBegin()
		sqlDB.ExpectCommit()

		Expect(WithTx(func(*sqlx.Tx) error { return nil }, dbx)).To(Succeed())
	})

	It("rolls back when the callback fails", func() {
		sqlDB.ExpectBegin()
		sqlDB.ExpectRollback()

		Expect(WithTx(func(*sqlx.Tx) error { return errors.New("boom") }, dbx)).To(MatchError("boom"))
	})

	It("returns begin failures", func() {
		sqlDB.ExpectBegin().WillReturnError(errors.New("begin failed"))

		Expect(WithTx(func(*sqlx.Tx) error { return nil }, dbx)).To(MatchError("begin failed"))
	})
})
