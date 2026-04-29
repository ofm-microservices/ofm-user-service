package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"user-service/config"
)

func TestYugabyteStorage(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Yugabyte Storage Suite")
}

var (
	storageSuiteContainer testcontainers.Container
	storageSuiteCfg       config.DBConfig
)

var _ = BeforeSuite(func() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	storageSuiteContainer, storageSuiteCfg = startStorageYugabyteContainer(ctx)
})

var _ = AfterSuite(func() {
	if storageSuiteContainer != nil {
		Expect(storageSuiteContainer.Terminate(context.Background())).To(Succeed())
	}
})

var _ = Describe("Open", func() {
	It("opens a real Yugabyte connection", func() {
		dbx, err := Open(storageSuiteCfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(dbx.Ping()).To(Succeed())
		Expect(dbx.Close()).To(Succeed())
	})

	It("wraps connection failures", func() {
		_, err := Open(config.DBConfig{
			Host:     "127.0.0.1",
			Port:     1,
			User:     "admin",
			Password: "admin",
			Name:     "user_service",
			SSLMode:  "disable",
		})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("open db"))
	})
})

var _ = Describe("RunMigrations", func() {
	It("applies migrations and tolerates no-change reruns", func() {
		cfg := storageSuiteCfg
		cfg.MigrationsPath = "file://" + filepath.Join(userServiceRoot(), "migration", "yugabyte")

		Expect(RunMigrations(cfg)).To(Succeed())
		Expect(RunMigrations(cfg)).To(Succeed())

		dbx, err := Open(cfg)
		Expect(err).NotTo(HaveOccurred())
		defer dbx.Close()

		var count int
		Expect(dbx.Get(&count, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'`)).To(Succeed())
		Expect(count).To(Equal(1))
	})

	It("wraps bad migration paths", func() {
		cfg := storageSuiteCfg
		cfg.MigrationsPath = "file:///definitely/missing"

		err := RunMigrations(cfg)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("create migrator"))
	})

	It("wraps migration execution failures", func() {
		tempDir, err := os.MkdirTemp("", "user-service-bad-migrations-*")
		Expect(err).NotTo(HaveOccurred())
		defer os.RemoveAll(tempDir)

		Expect(os.WriteFile(filepath.Join(tempDir, "000001_bad.up.sql"), []byte("THIS IS NOT SQL;"), 0o600)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(tempDir, "000001_bad.down.sql"), []byte("SELECT 1;"), 0o600)).To(Succeed())

		cfg := storageSuiteCfg
		cfg.MigrationsPath = "file://" + tempDir
		cfg.MigrationsTable = "schema_migrations_user_service_bad"

		err = RunMigrations(cfg)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("run migrations"))
	})

	It("resolves relative migration paths", func() {
		wd, err := os.Getwd()
		Expect(err).NotTo(HaveOccurred())
		Expect(os.Chdir(userServiceRoot())).To(Succeed())
		DeferCleanup(func() {
			Expect(os.Chdir(wd)).To(Succeed())
		})

		cfg := storageSuiteCfg
		cfg.MigrationsPath = "file://migration/yugabyte"
		cfg.MigrationsTable = "schema_migrations_user_service_relative"

		Expect(RunMigrations(cfg)).To(Succeed())
		Expect(RunMigrations(cfg)).To(Succeed())
	})
})

var _ = Describe("WithTx", func() {
	It("commits when the callback succeeds", func() {
		dbx, err := Open(storageSuiteCfg)
		Expect(err).NotTo(HaveOccurred())
		defer dbx.Close()

		Expect(RunMigrations(storageSuiteCfg)).To(Succeed())
		_, err = dbx.Exec(`TRUNCATE TABLE users`)
		Expect(err).NotTo(HaveOccurred())

		err = WithTx(func(tx *sqlx.Tx) error {
			_, execErr := tx.Exec(`INSERT INTO users (user_id, username, first_name, last_name) VALUES ($1, $2, $3, $4)`,
				"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "tx-user", "Tx", "User")
			return execErr
		}, dbx)
		Expect(err).NotTo(HaveOccurred())

		var count int
		Expect(dbx.Get(&count, `SELECT COUNT(*) FROM users WHERE user_id = $1`, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")).To(Succeed())
		Expect(count).To(Equal(1))
	})

	It("rolls back when the callback fails", func() {
		dbx, err := Open(storageSuiteCfg)
		Expect(err).NotTo(HaveOccurred())
		defer dbx.Close()

		Expect(RunMigrations(storageSuiteCfg)).To(Succeed())
		_, err = dbx.Exec(`TRUNCATE TABLE users`)
		Expect(err).NotTo(HaveOccurred())

		err = WithTx(func(tx *sqlx.Tx) error {
			_, execErr := tx.Exec(`INSERT INTO users (user_id, username, first_name, last_name) VALUES ($1, $2, $3, $4)`,
				"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "tx-user-rollback", "Tx", "Rollback")
			Expect(execErr).NotTo(HaveOccurred())
			return errors.New("boom")
		}, dbx)
		Expect(err).To(MatchError("boom"))

		var count int
		Expect(dbx.Get(&count, `SELECT COUNT(*) FROM users WHERE user_id = $1`, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")).To(Succeed())
		Expect(count).To(Equal(0))
	})
})

var _ = Describe("error wrappers", func() {
	It("preserves wrapped causes", func() {
		cause := errors.New("boom")

		Expect(WrapResolveMigrationsPathError(cause)).To(MatchError(ContainSubstring("resolve migrations path")))
		Expect(WrapCreateMigratorError(cause)).To(MatchError(ContainSubstring("create migrator")))
		Expect(WrapRunMigrationsError(cause)).To(MatchError(ContainSubstring("run migrations")))
		Expect(WrapOpenDBError(cause)).To(MatchError(ContainSubstring("open db")))
		Expect(WrapPingDBError(cause)).To(MatchError(ContainSubstring("ping db")))
	})
})

func startStorageYugabyteContainer(ctx context.Context) (testcontainers.Container, config.DBConfig) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "yugabytedb/yugabyte:2025.2.2.2-b11",
			ExposedPorts: []string{"5433/tcp"},
			Cmd:          []string{"bin/yugabyted", "start", "--daemon=false"},
			WaitingFor:   wait.ForListeningPort("5433/tcp").WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred())

	host, err := container.Host(ctx)
	Expect(err).NotTo(HaveOccurred())
	port, err := container.MappedPort(ctx, "5433/tcp")
	Expect(err).NotTo(HaveOccurred())

	adminDSN := fmt.Sprintf("postgres://yugabyte@%s:%s/yugabyte?sslmode=disable", host, port.Port())
	var adminDB *sqlx.DB
	Eventually(func() error {
		dbx, openErr := sqlx.Connect("pgx", adminDSN)
		if openErr != nil {
			return openErr
		}
		if pingErr := dbx.Ping(); pingErr != nil {
			_ = dbx.Close()
			return pingErr
		}
		adminDB = dbx
		return nil
	}, 90*time.Second, time.Second).Should(Succeed())
	defer func() {
		if adminDB != nil {
			_ = adminDB.Close()
		}
	}()

	_, err = adminDB.Exec(`
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin') THEN
		CREATE ROLE admin WITH LOGIN SUPERUSER PASSWORD 'admin';
	ELSE
		ALTER ROLE admin WITH LOGIN SUPERUSER PASSWORD 'admin';
	END IF;
END
$$;
`)
	Expect(err).NotTo(HaveOccurred())

	var exists int
	Expect(adminDB.Get(&exists, `SELECT COUNT(*) FROM pg_database WHERE datname = 'user_service'`)).To(Succeed())
	if exists == 0 {
		_, err = adminDB.Exec(`CREATE DATABASE user_service OWNER admin`)
		Expect(err).NotTo(HaveOccurred())
	}

	dbPort, err := strconv.Atoi(port.Port())
	Expect(err).NotTo(HaveOccurred())

	return container, config.DBConfig{
		Host:            host,
		Port:            dbPort,
		User:            "admin",
		Password:        "admin",
		Name:            "user_service",
		SSLMode:         "disable",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Minute,
		MigrationsPath:  "file://" + filepath.Join(userServiceRoot(), "migration", "yugabyte"),
		MigrationsTable: "schema_migrations_user_service",
	}
}

func userServiceRoot() string {
	_, file, _, ok := runtime.Caller(0)
	Expect(ok).To(BeTrue())
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../../"))
}
