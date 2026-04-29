package config

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestConfig(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Config Suite")
}

var _ = Describe("Load", func() {
	requiredKeys := []string{
		"DB_HOST",
		"DB_PORT",
		"DB_USER",
		"DB_PASSWORD",
		"DB_NAME",
		"REDIS_HOST",
		"REDIS_PORT",
		"NATS_URL",
	}

	setRequiredEnv := func() {
		Expect(os.Setenv("DB_HOST", "localhost")).To(Succeed())
		Expect(os.Setenv("DB_PORT", "5433")).To(Succeed())
		Expect(os.Setenv("DB_USER", "user_service")).To(Succeed())
		Expect(os.Setenv("DB_PASSWORD", "secret")).To(Succeed())
		Expect(os.Setenv("DB_NAME", "user_service")).To(Succeed())
		Expect(os.Setenv("REDIS_HOST", "localhost")).To(Succeed())
		Expect(os.Setenv("REDIS_PORT", "6379")).To(Succeed())
		Expect(os.Setenv("NATS_URL", "nats://localhost:4222")).To(Succeed())
	}

	BeforeEach(func() {
		for _, key := range requiredKeys {
			original, exists := os.LookupEnv(key)
			key := key
			DeferCleanup(func() {
				if exists {
					Expect(os.Setenv(key, original)).To(Succeed())
					return
				}
				Expect(os.Unsetenv(key)).To(Succeed())
			})

			Expect(os.Unsetenv(key)).To(Succeed())
		}
	})

	It("loads config from the environment and applies defaults", func() {
		setRequiredEnv()

		cfg, err := Load()

		Expect(err).NotTo(HaveOccurred())
		Expect(cfg).NotTo(BeNil())
		Expect(cfg.App.Env).To(Equal("local"))
		Expect(cfg.App.LogLevel).To(Equal("info"))
		Expect(cfg.DB.Host).To(Equal("localhost"))
		Expect(cfg.DB.Port).To(Equal(5433))
		Expect(cfg.DB.SSLMode).To(Equal("disable"))
		Expect(cfg.DB.ConnMaxLifetime).To(Equal(5 * time.Minute))
		Expect(cfg.DB.MigrationsPath).To(Equal("file://migration/yugabyte"))
		Expect(cfg.GRPC.Host).To(Equal("0.0.0.0"))
		Expect(cfg.GRPC.Port).To(Equal(9092))
		Expect(cfg.Redis.Host).To(Equal("localhost"))
		Expect(cfg.Redis.Port).To(Equal(6379))
		Expect(cfg.NATS.URL).To(Equal("nats://localhost:4222"))
		Expect(cfg.NATS.SagaCommandsStream).To(Equal("SAGA_USER_COMMANDS"))
	})

	It("wraps environment parsing failures", func() {
		Expect(os.Setenv("DB_PORT", "not-a-number")).To(Succeed())

		cfg, err := Load()

		Expect(cfg).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("parse env config"))
	})
})
