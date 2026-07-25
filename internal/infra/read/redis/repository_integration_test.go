package repository

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"user-service/config"
	user "user-service/internal/domain"
	readmodel "user-service/internal/infra/read/redis/model"
	pkgrdb "user-service/pkg/storage/redis"
)

func TestRedisRepository(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Redis Repository Suite")
}

var (
	redisSuiteContainer testcontainers.Container
	redisSuiteCfg       config.RedisConfig
	redisSuiteClient    *redis.Client
)

var _ = BeforeSuite(func() {
	if provider, err := testcontainers.ProviderDocker.GetProvider(); err != nil {
		return
	} else if err := provider.Health(context.Background()); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	redisSuiteContainer, redisSuiteCfg = startRedisContainer(ctx)
	var err error
	redisSuiteClient, err = pkgrdb.Open(ctx, redisSuiteCfg)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if redisSuiteClient != nil {
		Expect(redisSuiteClient.Close()).To(Succeed())
	}
	if redisSuiteContainer != nil {
		Expect(redisSuiteContainer.Terminate(context.Background())).To(Succeed())
	}
})

var _ = Describe("repository integration", func() {
	var logger logging.Logger

	BeforeEach(func() {
		if redisSuiteClient == nil {
			Skip("Docker is not available for the Redis suite")
		}
		var err error
		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
		Expect(redisSuiteClient.FlushDB(context.Background()).Err()).To(Succeed())
	})

	It("validates constructor dependencies", func() {
		repo, err := New(nil, logger)
		Expect(repo).To(BeNil())
		Expect(err).To(MatchError(ErrNilRedisClient))
	})

	It("upserts and deletes a projected user", func() {
		repoAny, err := New(redisSuiteClient, logger)
		Expect(err).NotTo(HaveOccurred())

		u := &user.User{
			ID:        "user-1",
			Username:  "alex",
			FirstName: "Alex",
			LastName:  "Doe",
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		}

		Expect(repoAny.Upsert(context.Background(), u)).To(Succeed())

		payload, err := redisSuiteClient.Get(context.Background(), UserPreviewCacheKey("user-1")).Bytes()
		Expect(err).NotTo(HaveOccurred())

		var cached readmodel.UserCache
		Expect(json.Unmarshal(payload, &cached)).To(Succeed())
		Expect(cached.ID).To(Equal("user-1"))
		Expect(cached.Username).To(Equal("alex"))

		Expect(repoAny.DeleteByID(context.Background(), "user-1")).To(Succeed())
		Expect(redisSuiteClient.Exists(context.Background(), UserPreviewCacheKey("user-1")).Val()).To(Equal(int64(0)))
	})

	It("rejects nil users", func() {
		repoAny, err := New(redisSuiteClient, logger)
		Expect(err).NotTo(HaveOccurred())

		Expect(repoAny.Upsert(context.Background(), nil)).To(MatchError(ErrNilUser))
	})

	It("builds stable cache keys", func() {
		Expect(UserPreviewCacheKey("user-1")).To(Equal("user:preview:user-1"))
	})
})

var _ = Describe("storage integration", func() {
	BeforeEach(func() {
		if redisSuiteClient == nil {
			Skip("Docker is not available for the Redis suite")
		}
	})

	It("opens a real redis connection", func() {
		client, err := pkgrdb.Open(context.Background(), redisSuiteCfg)

		Expect(err).NotTo(HaveOccurred())
		Expect(client.Ping(context.Background()).Err()).To(Succeed())
		Expect(client.Close()).To(Succeed())
	})

	It("wraps redis ping failures", func() {
		_, err := pkgrdb.Open(context.Background(), config.RedisConfig{
			Host: "127.0.0.1",
			Port: 1,
		})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("ping redis"))
	})
})

func startRedisContainer(ctx context.Context) (testcontainers.Container, config.RedisConfig) {
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

	return container, config.RedisConfig{
		Host: host,
		Port: redisPort,
		DB:   0,
	}
}
