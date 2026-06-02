package appfx

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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/fx/fxtest"
	"go.uber.org/mock/gomock"
	"user-service/config"
	app "user-service/internal/application"
	user "user-service/internal/domain"
	eventbroker "user-service/internal/presentation/event_broker"
	events "user-service/internal/presentation/event_broker/nats"
)

func TestFX(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "FX Suite")
}

var (
	fxNATSContainer  testcontainers.Container
	fxYBContainer    testcontainers.Container
	fxRedisContainer testcontainers.Container
	fxNATSCfg        config.NATSConfig
	fxYBCfg          config.DBConfig
	fxRedisCfg       config.RedisConfig
)

var _ = BeforeSuite(func() {
	if provider, err := testcontainers.ProviderDocker.GetProvider(); err != nil {
		return
	} else if err := provider.Health(context.Background()); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	fxNATSContainer, fxNATSCfg = startFXNATSContainer(ctx)
	fxYBContainer, fxYBCfg = startFXYugabyteContainer(ctx)
	fxRedisContainer, fxRedisCfg = startFXRedisContainer(ctx)
})

var _ = AfterSuite(func() {
	if fxNATSContainer != nil {
		Expect(fxNATSContainer.Terminate(context.Background())).To(Succeed())
	}
	if fxYBContainer != nil {
		Expect(fxYBContainer.Terminate(context.Background())).To(Succeed())
	}
	if fxRedisContainer != nil {
		Expect(fxRedisContainer.Terminate(context.Background())).To(Succeed())
	}
})

var _ = Describe("fx providers and invokes", func() {
	var (
		ctrl   *gomock.Controller
		logger logging.Logger
		cfg    *config.Config
		lc     *fxtest.Lifecycle
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())

		var err error
		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())

		cfg = &config.Config{
			App: config.AppConfig{
				Env:      "test",
				LogLevel: "debug",
			},
			DB: config.DBConfig{
				Host:     "localhost",
				Port:     5433,
				User:     "user_service",
				Password: "secret",
				Name:     "user_service",
			},
			GRPC: config.GRPCConfig{
				Host: "127.0.0.1",
				Port: 19592,
			},
			Redis: config.RedisConfig{
				Host: "localhost",
				Port: 6379,
			},
			NATS: config.NATSConfig{
				URL:                         "nats://localhost:4222",
				SagaCommandsStream:          "SAGA_USER_COMMANDS",
				SagaCreateUserSubject:       "saga.user.create",
				SagaDeleteUserSubject:       "saga.user.delete",
				SagaCreateUserResultSubject: "saga.user.create.result",
				SagaDeleteUserResultSubject: "saga.user.delete.result",
				SagaCreateUserDurable:       "user_create",
				SagaDeleteUserDurable:       "user_delete",
				SagaBatchSize:               1,
				SagaMaxWait:                 time.Millisecond,
				SagaWorkers:                 1,
				SagaQueueSize:               1,
				SagaAckWait:                 time.Second,
				SagaMaxDeliver:              1,
			},
		}

		lc = fxtest.NewLifecycle(GinkgoT())
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	It("logs startup", func() {
		InvokeStartLog(logger)
	})

	It("provides config from environment", func() {
		env := map[string]string{
			"DB_HOST":     "127.0.0.1",
			"DB_PORT":     "5433",
			"DB_USER":     "user_service",
			"DB_PASSWORD": "secret",
			"DB_NAME":     "user_service",
			"REDIS_HOST":  "127.0.0.1",
			"REDIS_PORT":  "6379",
			"NATS_URL":    "nats://127.0.0.1:4222",
		}

		for key, value := range env {
			original, exists := os.LookupEnv(key)
			key, value, original, exists := key, value, original, exists
			DeferCleanup(func() {
				if exists {
					Expect(os.Setenv(key, original)).To(Succeed())
					return
				}
				Expect(os.Unsetenv(key)).To(Succeed())
			})
			Expect(os.Setenv(key, value)).To(Succeed())
		}

		loaded, err := ProvideConfig()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.DB.Host).To(Equal("127.0.0.1"))
		Expect(loaded.Redis.Port).To(Equal(6379))
		Expect(loaded.NATS.URL).To(Equal("nats://127.0.0.1:4222"))
	})

	It("provides a logger and appends a stop hook", func() {
		provided, err := ProvideLogger(lc, cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(provided).NotTo(BeNil())
		Expect(lc.Stop(context.Background())).To(Succeed())
	})

	It("fails logger construction on invalid log levels", func() {
		badCfg := *cfg
		badCfg.App.LogLevel = "definitely-invalid"

		provided, err := ProvideLogger(lc, &badCfg)
		Expect(provided).To(BeNil())
		Expect(err).To(HaveOccurred())
	})

	It("constructs the application service", func() {
		svc, err := ProvideUserService(&stubWriteRepo{}, &stubReadRepo{}, &stubFileClient{}, &stubDetailedUserPublisher{}, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(svc).NotTo(BeNil())
	})

	It("propagates repository constructor validation", func() {
		writeRepo, err := ProvideWriteRepo(nil, nil, logger)
		Expect(writeRepo).To(BeNil())
		Expect(err).To(MatchError("yugabyte db is nil"))
	})

	It("propagates read repository constructor validation", func() {
		readRepo, err := ProvideReadRepo(nil, logger)
		Expect(readRepo).To(BeNil())
		Expect(err).To(MatchError("redis client is nil"))
	})

	It("constructs presentation adapters", func() {
		service := &stubUserService{}
		broker := &stubEventBroker{}

		subscriber, err := ProvideRegistrationSagaSubscriber(broker, service, cfg, events.NewDomainFailureReasonResolver(), logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(subscriber).NotTo(BeNil())

		server, err := ProvideGRPCServer(service, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(server).NotTo(BeNil())
	})

	It("registers subscriber lifecycle hooks and runs start-stop cleanly", func() {
		subscriber := &registrationSagaSubscriberStub{}

		InvokeSubscribeRegistrationSaga(lc, subscriber, cfg, logger)

		Expect(lc.Start(context.Background())).To(Succeed())
		Expect(lc.Stop(context.Background())).To(Succeed())
		Expect(subscriber.calls).To(Equal(1))
	})

	It("propagates subscriber startup failures", func() {
		subscriber := &registrationSagaSubscriberStub{err: errors.New("boom")}

		InvokeSubscribeRegistrationSaga(lc, subscriber, cfg, logger)

		Expect(lc.Start(context.Background())).To(MatchError("boom"))
		Expect(subscriber.calls).To(Equal(1))
	})

	It("registers grpc lifecycle hooks and stops the server", func() {
		server := NewMockServer(ctrl)
		started := make(chan struct{}, 1)

		server.EXPECT().Start().DoAndReturn(func() error {
			started <- struct{}{}
			return nil
		})
		server.EXPECT().Shutdown(gomock.Any()).Return(nil)

		InvokeRunGRPCServer(lc, server)

		Expect(lc.Start(context.Background())).To(Succeed())
		Eventually(started).Should(Receive())
		Expect(lc.Stop(context.Background())).To(Succeed())
	})

	It("ensures streams and opens an event broker against real nats", func() {
		if fxNATSContainer == nil {
			Skip("Docker is not available for the FX NATS suite")
		}
		cfg.NATS = fxNATSCfg

		Expect(InvokeEnsureStream(cfg, logger)).To(Succeed())

		eventBroker, err := ProvideEventBroker(lc, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(eventBroker).NotTo(BeNil())
		Expect(lc.Stop(context.Background())).To(Succeed())
	})

	It("propagates nats bootstrap and broker construction failures", func() {
		badCfg := *cfg
		badCfg.NATS = config.NATSConfig{
			URL:                         "nats://127.0.0.1:1",
			UserEventsStream:            "USER_EVENTS",
			UserCreatedSubject:          "user.created",
			SagaCommandsStream:          "SAGA_USER_COMMANDS",
			SagaCreateUserSubject:       "saga.user.create",
			SagaDeleteUserSubject:       "saga.user.delete",
			SagaCreateUserResultSubject: "saga.user.create.result",
			SagaDeleteUserResultSubject: "saga.user.delete.result",
		}

		Expect(InvokeEnsureStream(&badCfg, logger)).To(HaveOccurred())

		badCfg.NATS.URL = ""
		eventBroker, err := ProvideEventBroker(lc, &badCfg, logger)
		Expect(eventBroker).To(BeNil())
		Expect(err).To(MatchError("nats url is empty"))
	})

	It("runs migrations and opens a real yugabyte connection", func() {
		if fxYBContainer == nil {
			Skip("Docker is not available for the FX Yugabyte suite")
		}
		cfg.DB = fxYBCfg

		Expect(InvokeRunMigrations(cfg, logger)).To(Succeed())

		dbx, err := ProvideYugaByteDB(lc, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(dbx.Ping()).To(Succeed())
		Expect(lc.Stop(context.Background())).To(Succeed())
	})

	It("propagates migration and database open failures", func() {
		badCfg := *cfg
		badCfg.DB = config.DBConfig{
			Host:            "127.0.0.1",
			Port:            1,
			User:            "admin",
			Password:        "admin",
			Name:            "user_service",
			SSLMode:         "disable",
			MigrationsPath:  "bad://migration-source",
			MigrationsTable: "schema_migrations_user_service",
		}

		Expect(InvokeRunMigrations(&badCfg, logger)).To(HaveOccurred())

		dbx, err := ProvideYugaByteDB(lc, &badCfg, logger)
		Expect(dbx).To(BeNil())
		Expect(err).To(HaveOccurred())
	})

	It("opens a real redis connection", func() {
		if fxRedisContainer == nil {
			Skip("Docker is not available for the FX Redis suite")
		}
		cfg.Redis = fxRedisCfg

		client, err := ProvideRedisClient(lc, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Ping(context.Background()).Err()).To(Succeed())
		Expect(lc.Stop(context.Background())).To(Succeed())
	})

	It("propagates redis open failures", func() {
		badCfg := *cfg
		badCfg.Redis = config.RedisConfig{
			Host: "127.0.0.1",
			Port: 1,
			DB:   0,
		}

		client, err := ProvideRedisClient(lc, &badCfg, logger)
		Expect(client).To(BeNil())
		Expect(err).To(HaveOccurred())
	})

	It("covers storage and messaging provider success paths with seams", func() {
		previousRunMigrations := runMigrations
		previousOpenYugaByteDB := openYugaByteDB
		previousOpenRedisClient := openRedisClient
		previousEnsureStream := ensureStream
		previousNewEventBroker := newEventBroker
		defer func() {
			runMigrations = previousRunMigrations
			openYugaByteDB = previousOpenYugaByteDB
			openRedisClient = previousOpenRedisClient
			ensureStream = previousEnsureStream
			newEventBroker = previousNewEventBroker
		}()

		runMigrations = func(config.DBConfig) error { return nil }
		Expect(InvokeRunMigrations(cfg, logger)).To(Succeed())

		rawDB, mock, err := sqlmock.New()
		Expect(err).NotTo(HaveOccurred())
		dbx := sqlx.NewDb(rawDB, "sqlmock")
		openYugaByteDB = func(config.DBConfig) (*sqlx.DB, error) { return dbx, nil }
		dbProvided, err := ProvideYugaByteDB(lc, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(dbProvided).To(Equal(dbx))
		mock.ExpectClose()

		redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
		openRedisClient = func(context.Context, config.RedisConfig) (*redis.Client, error) { return redisClient, nil }
		redisProvided, err := ProvideRedisClient(lc, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(redisProvided).To(Equal(redisClient))

		ensureStream = func(config.NATSConfig, logging.Logger) error { return nil }
		Expect(InvokeEnsureStream(cfg, logger)).To(Succeed())

		stubBroker := &stubEventBroker{}
		newEventBroker = func(config.NATSConfig, logging.Logger) (eventbroker.EventBroker, error) {
			return stubBroker, nil
		}
		providedBroker, err := ProvideEventBroker(lc, cfg, logger)
		Expect(err).NotTo(HaveOccurred())
		Expect(providedBroker).To(Equal(stubBroker))

		Expect(lc.Start(context.Background())).To(Succeed())
		Expect(lc.Stop(context.Background())).To(Succeed())
		Expect(mock.ExpectationsWereMet()).To(Succeed())
	})
})

type stubWriteRepo struct{}

func (s *stubWriteRepo) Create(context.Context, user.CreateUserParams) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubWriteRepo) GetByID(context.Context, string) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubWriteRepo) GetByUsername(context.Context, string) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubWriteRepo) ExistsByUsername(context.Context, string) (bool, error) { return false, nil }
func (s *stubWriteRepo) ActivateByID(context.Context, string) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubWriteRepo) DeactivateByID(context.Context, string) error { return nil }
func (s *stubWriteRepo) DeleteByID(context.Context, string) error     { return nil }

type stubReadRepo struct{}

func (s *stubReadRepo) Upsert(context.Context, *user.User) error { return nil }
func (s *stubReadRepo) GetByID(context.Context, string) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubReadRepo) DeleteByID(context.Context, string) error           { return nil }
func (s *stubReadRepo) UpsertByUsername(context.Context, *user.User) error { return nil }
func (s *stubReadRepo) GetByUsername(context.Context, string) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubReadRepo) DeleteByUsername(context.Context, string) error { return nil }

type stubFileClient struct{}

func (s *stubFileClient) GetFileURL(context.Context, string) (string, error) { return "", nil }

type stubDetailedUserPublisher struct{}

func (s *stubDetailedUserPublisher) PublishDetailedUserRequested(context.Context, *user.User) error {
	return nil
}

type stubUserService struct{}

func (s *stubUserService) CreateUser(context.Context, string, string, string, string) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubUserService) ExistsByUsername(context.Context, string) (bool, error) { return false, nil }
func (s *stubUserService) ActivateUser(context.Context, string) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubUserService) DeactivateUser(context.Context, string) error { return nil }
func (s *stubUserService) DeleteUser(context.Context, string) error     { return nil }
func (s *stubUserService) GetUserPreviewByID(context.Context, string) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubUserService) GetUserPreviewByIDNoCache(context.Context, string) (*user.User, error) {
	return &user.User{}, nil
}
func (s *stubUserService) GetDetailedUserByUsername(context.Context, string) (*user.User, error) {
	return &user.User{}, nil
}

type stubEventBroker struct{}

func (s *stubEventBroker) Publish(context.Context, string, []byte) error { return nil }
func (s *stubEventBroker) Subscribe(context.Context, string, eventbroker.MessageHandler) error {
	return nil
}
func (s *stubEventBroker) RunPullConsumer(context.Context, config.PullConsumerConfig, eventbroker.MessageHandler) error {
	return nil
}
func (s *stubEventBroker) Close() {}

type registrationSagaSubscriberStub struct {
	err   error
	calls int
}

func (s *registrationSagaSubscriberStub) Subscribe(context.Context) error {
	s.calls++
	return s.err
}

var (
	_ user.UserRepository       = (*stubWriteRepo)(nil)
	_ user.UserReadRepository   = (*stubReadRepo)(nil)
	_ app.UserService           = (*stubUserService)(nil)
	_ app.FileURLClient         = (*stubFileClient)(nil)
	_ app.DetailedUserPublisher = (*stubDetailedUserPublisher)(nil)
	_ eventbroker.EventBroker   = (*stubEventBroker)(nil)
)

func startFXNATSContainer(ctx context.Context) (testcontainers.Container, config.NATSConfig) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.12.4-alpine",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"-js"},
			WaitingFor:   wait.ForListeningPort("4222/tcp"),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred())

	host, err := container.Host(ctx)
	Expect(err).NotTo(HaveOccurred())
	port, err := container.MappedPort(ctx, "4222/tcp")
	Expect(err).NotTo(HaveOccurred())

	return container, config.NATSConfig{
		URL:                              "nats://" + host + ":" + port.Port(),
		UserEventsStream:                 "USER_EVENTS",
		UserCreatedSubject:               "user.created",
		SagaCommandsStream:               "SAGA_USER_COMMANDS",
		SagaCreateUserSubject:            "saga.user.create",
		SagaDeleteUserSubject:            "saga.user.delete",
		SagaCreateUserResultSubject:      "saga.user.create.result",
		SagaDeleteUserResultSubject:      "saga.user.delete.result",
		SagaCreateUserDurable:            "user_service_saga_create",
		SagaDeleteUserDurable:            "user_service_saga_delete",
		SagaBatchSize:                    1,
		SagaMaxWait:                      time.Millisecond,
		SagaWorkers:                      1,
		SagaQueueSize:                    1,
		SagaAckWait:                      time.Second,
		SagaMaxDeliver:                   1,
		UserDetailedStream:               "USER_DETAILED",
		UserDetailedRequestedSubject:     "user.detailed.requested",
		UserDetailedProjectionSubject:    "user.detailed.projection.requested",
		UserDetailedProjectionDurable:    "user_service_user_detailed_projection",
		UserDetailedProjectionBatchSize:  1,
		UserDetailedProjectionMaxWait:    time.Millisecond,
		UserDetailedProjectionWorkers:    1,
		UserDetailedProjectionQueueSize:  1,
		UserDetailedProjectionAckWait:    time.Second,
		UserDetailedProjectionMaxDeliver: 1,
	}
}

func startFXRedisContainer(ctx context.Context) (testcontainers.Container, config.RedisConfig) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7.4-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	Expect(err).NotTo(HaveOccurred())

	host, err := container.Host(ctx)
	Expect(err).NotTo(HaveOccurred())
	port, err := container.MappedPort(ctx, "6379/tcp")
	Expect(err).NotTo(HaveOccurred())
	redisPort, err := strconv.Atoi(port.Port())
	Expect(err).NotTo(HaveOccurred())

	return container, config.RedisConfig{Host: host, Port: redisPort, DB: 0}
}

func startFXYugabyteContainer(ctx context.Context) (testcontainers.Container, config.DBConfig) {
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
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../"))
}
