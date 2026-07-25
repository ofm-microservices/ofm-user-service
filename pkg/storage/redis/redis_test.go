package redis

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"user-service/config"
)

func TestRedisStorage(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Redis Storage Suite")
}

var (
	redisStorageSuiteContainer testcontainers.Container
	redisStorageSuiteCfg       config.RedisConfig
)

var _ = BeforeSuite(func() {
	if provider, err := testcontainers.ProviderDocker.GetProvider(); err != nil {
		return
	} else if err := provider.Health(context.Background()); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	redisStorageSuiteContainer, redisStorageSuiteCfg = startRedisStorageContainer(ctx)
})

var _ = AfterSuite(func() {
	if redisStorageSuiteContainer != nil {
		Expect(redisStorageSuiteContainer.Terminate(context.Background())).To(Succeed())
	}
})

var _ = Describe("Open", func() {
	It("opens a real redis connection", func() {
		if redisStorageSuiteContainer == nil {
			Skip("Docker is not available for the Redis storage suite")
		}
		client, err := Open(context.Background(), redisStorageSuiteCfg)

		Expect(err).NotTo(HaveOccurred())
		Expect(client.Ping(context.Background()).Err()).To(Succeed())
		Expect(client.Close()).To(Succeed())
	})

	It("wraps redis ping failures", func() {
		_, err := Open(context.Background(), config.RedisConfig{
			Host: "127.0.0.1",
			Port: 1,
		})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("ping redis"))
	})
})

var _ = Describe("WrapRedisPingError", func() {
	It("preserves the wrapped cause", func() {
		err := WrapRedisPingError(errors.New("boom"))

		Expect(err).To(MatchError(ContainSubstring("ping redis")))
		Expect(err).To(MatchError(ContainSubstring("boom")))
	})
})

func startRedisStorageContainer(ctx context.Context) (testcontainers.Container, config.RedisConfig) {
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
