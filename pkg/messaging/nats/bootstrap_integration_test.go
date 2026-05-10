package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"user-service/config"
)

type fakeBootstrapConn struct {
	js     jetStreamManager
	err    error
	closed bool
}

func (c *fakeBootstrapConn) JetStream() (jetStreamManager, error) {
	return c.js, c.err
}

func (c *fakeBootstrapConn) Close() {
	c.closed = true
}

type fakeJetStream struct {
	addErrors    []error
	updateErrors []error
	added        []string
	updated      []string
}

func (js *fakeJetStream) AddStream(cfg *nats.StreamConfig, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	js.added = append(js.added, cfg.Name)
	if len(js.addErrors) == 0 {
		return &nats.StreamInfo{Config: *cfg}, nil
	}
	err := js.addErrors[0]
	js.addErrors = js.addErrors[1:]
	return nil, err
}

func (js *fakeJetStream) UpdateStream(cfg *nats.StreamConfig, _ ...nats.JSOpt) (*nats.StreamInfo, error) {
	js.updated = append(js.updated, cfg.Name)
	if len(js.updateErrors) == 0 {
		return &nats.StreamInfo{Config: *cfg}, nil
	}
	err := js.updateErrors[0]
	js.updateErrors = js.updateErrors[1:]
	return nil, err
}

func TestBootstrap(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "NATS Bootstrap Suite")
}

var (
	bootstrapJSContainer   testcontainers.Container
	bootstrapNoJSContainer testcontainers.Container
	bootstrapJSCfg         config.NATSConfig
	bootstrapNoJSCfg       config.NATSConfig
	bootstrapLogger        logging.Logger
)

var _ = BeforeSuite(func() {
	if provider, err := testcontainers.ProviderDocker.GetProvider(); err != nil {
		return
	} else if err := provider.Health(context.Background()); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	var err error
	bootstrapLogger, err = logging.New("user-service", "test", "debug")
	Expect(err).NotTo(HaveOccurred())

	bootstrapJSContainer, bootstrapJSCfg = startBootstrapNATSContainer(ctx, true)
	bootstrapNoJSContainer, bootstrapNoJSCfg = startBootstrapNATSContainer(ctx, false)
})

var _ = AfterSuite(func() {
	if bootstrapJSContainer != nil {
		Expect(bootstrapJSContainer.Terminate(context.Background())).To(Succeed())
	}
	if bootstrapNoJSContainer != nil {
		Expect(bootstrapNoJSContainer.Terminate(context.Background())).To(Succeed())
	}
})

var _ = Describe("bootstrap integration", func() {
	BeforeEach(func() {
		if bootstrapJSContainer == nil {
			Skip("Docker is not available for the NATS bootstrap suite")
		}
	})

	It("connects to a real nats server", func() {
		nc, err := Connect(bootstrapJSCfg)

		Expect(err).NotTo(HaveOccurred())
		Expect(nc.IsConnected()).To(BeTrue())
		nc.Close()
	})

	It("connects when optional credentials are present in config", func() {
		cfg := bootstrapJSCfg
		cfg.User = "ignored-user"
		cfg.Password = "ignored-pass"

		nc, err := Connect(cfg)

		Expect(err).NotTo(HaveOccurred())
		Expect(nc.IsConnected()).To(BeTrue())
		nc.Close()
	})

	It("creates and then updates the required streams", func() {
		Expect(EnsureStream(bootstrapJSCfg, bootstrapLogger)).To(Succeed())
		Expect(EnsureStream(bootstrapJSCfg, bootstrapLogger)).To(Succeed())

		nc, err := nats.Connect(bootstrapJSCfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer nc.Close()

		js, err := nc.JetStream()
		Expect(err).NotTo(HaveOccurred())

		userEventsInfo, err := js.StreamInfo(bootstrapJSCfg.UserEventsStream)
		Expect(err).NotTo(HaveOccurred())
		Expect(userEventsInfo.Config.Subjects).To(ContainElements(
			bootstrapJSCfg.UserCreatedSubject,
			bootstrapJSCfg.SagaCreateUserResultSubject,
			bootstrapJSCfg.SagaDeleteUserResultSubject,
		))

		sagaCommandsInfo, err := js.StreamInfo(bootstrapJSCfg.SagaCommandsStream)
		Expect(err).NotTo(HaveOccurred())
		Expect(sagaCommandsInfo.Config.Subjects).To(ContainElements(
			bootstrapJSCfg.SagaCreateUserSubject,
			bootstrapJSCfg.SagaDeleteUserSubject,
		))
	})

	It("requires a logger", func() {
		Expect(EnsureStream(bootstrapJSCfg, nil)).To(MatchError(ErrNilLogger))
	})

	It("wraps jetstream initialization failures when the server has no jetstream", func() {
		err := EnsureStream(bootstrapNoJSCfg, bootstrapLogger)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`ensure stream "USER_EVENTS"`))
	})
})

var _ = Describe("bootstrap unit", func() {
	var logger logging.Logger

	BeforeEach(func() {
		var err error
		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		connectBootstrap = func(cfg config.NATSConfig) (bootstrapConn, error) {
			nc, err := Connect(cfg)
			if err != nil {
				return nil, err
			}
			return realBootstrapConn{Conn: nc}, nil
		}
	})

	It("requires a logger before connecting", func() {
		Expect(EnsureStream(config.NATSConfig{}, nil)).To(MatchError(ErrNilLogger))
	})

	It("creates streams and closes the connection", func() {
		js := &fakeJetStream{}
		conn := &fakeBootstrapConn{js: js}
		connectBootstrap = func(config.NATSConfig) (bootstrapConn, error) {
			return conn, nil
		}

		Expect(EnsureStream(config.NATSConfig{
			UserEventsStream:            "USER_EVENTS",
			UserCreatedSubject:          "user.created",
			SagaCreateUserResultSubject: "saga.user.create.result",
			SagaDeleteUserResultSubject: "saga.user.delete.result",
			SagaCommandsStream:          "SAGA_USER_COMMANDS",
			SagaCreateUserSubject:       "saga.user.create",
			SagaDeleteUserSubject:       "saga.user.delete",
		}, logger)).To(Succeed())

		Expect(js.added).To(Equal([]string{"USER_EVENTS", "SAGA_USER_COMMANDS"}))
		Expect(conn.closed).To(BeTrue())
	})

	It("updates streams when add reports an existing stream", func() {
		js := &fakeJetStream{addErrors: []error{errors.New("exists"), errors.New("exists")}}
		connectBootstrap = func(config.NATSConfig) (bootstrapConn, error) {
			return &fakeBootstrapConn{js: js}, nil
		}

		Expect(EnsureStream(config.NATSConfig{
			UserEventsStream:   "USER_EVENTS",
			SagaCommandsStream: "SAGA_USER_COMMANDS",
		}, logger)).To(Succeed())

		Expect(js.updated).To(Equal([]string{"USER_EVENTS", "SAGA_USER_COMMANDS"}))
	})

	It("wraps connection, jetstream, and stream update failures", func() {
		connectBootstrap = func(config.NATSConfig) (bootstrapConn, error) {
			return nil, errors.New("connect failed")
		}
		Expect(EnsureStream(config.NATSConfig{}, logger)).To(MatchError("connect failed"))

		connectBootstrap = func(config.NATSConfig) (bootstrapConn, error) {
			return &fakeBootstrapConn{err: errors.New("jetstream failed")}, nil
		}
		Expect(EnsureStream(config.NATSConfig{}, logger)).To(MatchError(ContainSubstring("init jetstream context")))

		js := &fakeJetStream{
			addErrors:    []error{errors.New("exists")},
			updateErrors: []error{errors.New("update failed")},
		}
		connectBootstrap = func(config.NATSConfig) (bootstrapConn, error) {
			return &fakeBootstrapConn{js: js}, nil
		}
		Expect(EnsureStream(config.NATSConfig{UserEventsStream: "USER_EVENTS"}, logger)).
			To(MatchError(ContainSubstring(`ensure stream "USER_EVENTS"`)))
	})

	It("wraps connect failures and accepts credential options", func() {
		_, err := Connect(config.NATSConfig{
			URL:      "nats://127.0.0.1:1",
			User:     "user",
			Password: "pass",
		})

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("connect to nats"))
	})
})

var _ = Describe("error wrappers", func() {
	It("preserves wrapped causes", func() {
		cause := errors.New("boom")

		Expect(WrapInitJetStreamContextError(cause)).To(MatchError(ContainSubstring("init jetstream context")))
		Expect(WrapEnsureStreamError("STREAM", cause, cause)).To(MatchError(ContainSubstring(`ensure stream "STREAM"`)))
		Expect(WrapConnectToNATSError(cause)).To(MatchError(ContainSubstring("connect to nats")))
	})
})

func startBootstrapNATSContainer(ctx context.Context, jetstream bool) (testcontainers.Container, config.NATSConfig) {
	cmd := []string{}
	if jetstream {
		cmd = []string{"-js"}
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.12.4-alpine",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          cmd,
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
		URL:                         "nats://" + host + ":" + port.Port(),
		UserEventsStream:            "USER_EVENTS",
		UserCreatedSubject:          "user.created",
		SagaCommandsStream:          "SAGA_USER_COMMANDS",
		SagaCreateUserSubject:       "saga.user.create",
		SagaDeleteUserSubject:       "saga.user.delete",
		SagaCreateUserResultSubject: "saga.user.create.result",
		SagaDeleteUserResultSubject: "saga.user.delete.result",
	}
}
