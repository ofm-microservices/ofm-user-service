package db

import (
	"errors"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang-migrate/migrate/v4"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"user-service/config"
)

type fakeMigrator struct {
	err error
}

func (m fakeMigrator) Up() error {
	return m.err
}

var _ = Describe("RunMigrations unit", func() {
	AfterEach(func() {
		newMigrator = func(sourceURL, databaseURL string) (migrator, error) {
			return migrate.New(sourceURL, databaseURL)
		}
	})

	It("creates migrators with escaped DSNs and applies migrations", func() {
		var sourceURL string
		var databaseURL string
		newMigrator = func(source, database string) (migrator, error) {
			sourceURL = source
			databaseURL = database
			return fakeMigrator{}, nil
		}

		err := RunMigrations(config.DBConfig{
			Host:            "localhost",
			Port:            5433,
			User:            "user name",
			Password:        "p@ss word",
			Name:            "user_service",
			SSLMode:         "disable",
			MigrationsPath:  "bad://source",
			MigrationsTable: "schema migrations",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(sourceURL).To(Equal("bad://source"))
		Expect(databaseURL).To(ContainSubstring("user+name:p%40ss+word"))
		Expect(databaseURL).To(ContainSubstring("x-migrations-table=schema+migrations"))
	})

	It("resolves relative file paths and tolerates no-change", func() {
		var sourceURL string
		newMigrator = func(source string, _ string) (migrator, error) {
			sourceURL = source
			return fakeMigrator{err: migrate.ErrNoChange}, nil
		}

		err := RunMigrations(config.DBConfig{
			Host:            "localhost",
			Port:            5433,
			User:            "admin",
			Password:        "admin",
			Name:            "user_service",
			SSLMode:         "disable",
			MigrationsPath:  "file://migration/yugabyte",
			MigrationsTable: "schema_migrations",
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(sourceURL).To(HavePrefix("file:///"))
	})

	It("wraps migrator creation and migration run failures", func() {
		newMigrator = func(string, string) (migrator, error) {
			return nil, errors.New("create failed")
		}
		Expect(RunMigrations(config.DBConfig{MigrationsPath: "bad://source"})).To(MatchError(ContainSubstring("create migrator")))

		newMigrator = func(string, string) (migrator, error) {
			return fakeMigrator{err: errors.New("up failed")}, nil
		}
		Expect(RunMigrations(config.DBConfig{MigrationsPath: "bad://source"})).To(MatchError(ContainSubstring("run migrations")))
	})
})

var _ = Describe("Open unit", func() {
	AfterEach(func() {
		connectDB = sqlx.Connect
	})

	It("configures a successful connection pool", func() {
		rawDB, _, err := sqlmock.New()
		Expect(err).NotTo(HaveOccurred())
		dbx := sqlx.NewDb(rawDB, "sqlmock")
		defer rawDB.Close()

		connectDB = func(driverName, dsn string) (*sqlx.DB, error) {
			Expect(driverName).To(Equal("pgx"))
			Expect(dsn).To(ContainSubstring("postgres://admin:admin@localhost:5433/user_service?sslmode=disable"))
			return dbx, nil
		}

		opened, err := Open(config.DBConfig{
			Host:            "localhost",
			Port:            5433,
			User:            "admin",
			Password:        "admin",
			Name:            "user_service",
			SSLMode:         "disable",
			MaxOpenConns:    7,
			MaxIdleConns:    3,
			ConnMaxLifetime: time.Minute,
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(opened).To(Equal(dbx))
	})

	It("wraps connection failures", func() {
		connectDB = func(string, string) (*sqlx.DB, error) {
			return nil, errors.New("connect failed")
		}

		dbx, err := Open(config.DBConfig{})
		Expect(dbx).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("open db")))
	})
})
