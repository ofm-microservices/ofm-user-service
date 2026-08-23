package repository

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"user-service/config"
	user "user-service/internal/domain"
	pkgdb "user-service/pkg/storage/yugabyte"
)

var (
	repoSuiteContainer testcontainers.Container
	repoSuiteCfg       config.DBConfig
	repoSuiteDB        *sqlx.DB
)

var _ = Describe("repository integration", Ordered, func() {
	var repoAny user.UserRepository
	var logger logging.Logger

	BeforeAll(func() {
		if provider, err := testcontainers.ProviderDocker.GetProvider(); err != nil {
			Skip("Docker is not available for the Yugabyte suite")
		} else if err := provider.Health(context.Background()); err != nil {
			Skip("Docker is not healthy for the Yugabyte suite")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		repoSuiteContainer, repoSuiteCfg = startYugabyteContainer(ctx)
		Expect(pkgdb.RunMigrations(repoSuiteCfg)).To(Succeed())

		var err error
		repoSuiteDB, err = pkgdb.Open(repoSuiteCfg)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if repoSuiteDB != nil {
			Expect(repoSuiteDB.Close()).To(Succeed())
		}
		if repoSuiteContainer != nil {
			Expect(repoSuiteContainer.Terminate(context.Background())).To(Succeed())
		}
	})

	BeforeEach(func() {
		_, err := repoSuiteDB.Exec(`TRUNCATE TABLE outbox_events, users CASCADE`)
		Expect(err).NotTo(HaveOccurred())

		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
		var errNew error
		repoAny, errNew = New(repoSuiteDB, NewPgErrorTranslator(), logger)
		Expect(errNew).NotTo(HaveOccurred())
	})

	It("validates constructor dependencies", func() {
		repo, err := New(nil, NewPgErrorTranslator(), logger)
		Expect(repo).To(BeNil())
		Expect(err).To(MatchError(ErrNilYugaByteDB))

		repo, err = New(repoSuiteDB, nil, logger)
		Expect(repo).To(BeNil())
		Expect(err).To(MatchError(ErrNilDBErrorTranslator))
	})

	It("creates and loads users", func() {
		created, err := repoAny.Create(context.Background(), user.CreateUserParams{
			ID:        "11111111-1111-1111-1111-111111111111",
			Username:  "alex",
			FirstName: "Alex",
			LastName:  "Doe",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(created.CreatedAt.IsZero()).To(BeFalse())
		Expect(created.UpdatedAt.IsZero()).To(BeFalse())

		loaded, err := repoAny.GetByID(context.Background(), "11111111-1111-1111-1111-111111111111")
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.ID).To(Equal(created.ID))
		Expect(loaded.Username).To(Equal("alex"))
		Expect(loaded.FirstName).To(Equal("Alex"))
		Expect(loaded.LastName).To(Equal("Doe"))
	})

	It("captures insert update and delete in the transactional outbox", func() {
		userID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
		_, err := repoSuiteDB.Exec(`INSERT INTO users (user_id, username, first_name, last_name, status) VALUES ($1, $2, $3, $4, 'active')`, userID, "outbox-user", "Outbox", "User")
		Expect(err).NotTo(HaveOccurred())
		_, err = repoSuiteDB.Exec(`UPDATE users SET first_name = 'Updated' WHERE user_id = $1`, userID)
		Expect(err).NotTo(HaveOccurred())
		_, err = repoSuiteDB.Exec(`DELETE FROM users WHERE user_id = $1`, userID)
		Expect(err).NotTo(HaveOccurred())

		var operations []string
		Expect(repoSuiteDB.Select(&operations, `SELECT operation FROM outbox_events WHERE aggregate_id = $1 ORDER BY occurred_at, created_at`, userID)).To(Succeed())
		Expect(operations).To(Equal([]string{"created", "updated", "deactivated"}))
	})

	It("reports username existence", func() {
		_, err := repoAny.Create(context.Background(), user.CreateUserParams{
			ID:       "22222222-2222-2222-2222-222222222222",
			Username: "exists",
		})
		Expect(err).NotTo(HaveOccurred())

		exists, err := repoAny.ExistsByUsername(context.Background(), "exists")
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeTrue())

		exists, err = repoAny.ExistsByUsername(context.Background(), "missing")
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse())
	})

	It("wraps canceled query execution", func() {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		exists, err := repoAny.ExistsByUsername(canceledCtx, "exists")
		Expect(exists).To(BeFalse())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(user.ErrFailedToFindUser.Error()))
	})

	It("maps duplicate ids, duplicate usernames, and invalid ids", func() {
		_, err := repoAny.Create(context.Background(), user.CreateUserParams{
			ID:       "33333333-3333-3333-3333-333333333333",
			Username: "taken",
		})
		Expect(err).NotTo(HaveOccurred())

		created, err := repoAny.Create(context.Background(), user.CreateUserParams{
			ID:       "33333333-3333-3333-3333-333333333333",
			Username: "other",
		})
		Expect(created).To(BeNil())
		Expect(err).To(MatchError(user.ErrUserIDAlreadyTaken))

		created, err = repoAny.Create(context.Background(), user.CreateUserParams{
			ID:       "44444444-4444-4444-4444-444444444444",
			Username: "taken",
		})
		Expect(created).To(BeNil())
		Expect(err).To(MatchError(user.ErrUsernameAlreadyTaken))

		created, err = repoAny.Create(context.Background(), user.CreateUserParams{
			ID:       "not-a-uuid",
			Username: "broken",
		})
		Expect(created).To(BeNil())
		Expect(err).To(MatchError(user.ErrInvalidUserID))
	})

	It("maps lookup and delete misses to user not found", func() {
		loaded, err := repoAny.GetByID(context.Background(), "66666666-6666-6666-6666-666666666666")
		Expect(loaded).To(BeNil())
		Expect(err).To(MatchError(user.ErrUserNotFound))

		err = repoAny.DeleteByID(context.Background(), "66666666-6666-6666-6666-666666666666")
		Expect(err).To(MatchError(user.ErrUserNotFound))
	})

	It("deletes users", func() {
		_, err := repoAny.Create(context.Background(), user.CreateUserParams{
			ID:       "77777777-7777-7777-7777-777777777777",
			Username: "delete-me",
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(repoAny.DeleteByID(context.Background(), "77777777-7777-7777-7777-777777777777")).To(Succeed())

		loaded, err := repoAny.GetByID(context.Background(), "77777777-7777-7777-7777-777777777777")
		Expect(loaded).To(BeNil())
		Expect(err).To(MatchError(user.ErrUserNotFound))
	})
})

func startYugabyteContainer(ctx context.Context) (testcontainers.Container, config.DBConfig) {
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
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../../../"))
}
