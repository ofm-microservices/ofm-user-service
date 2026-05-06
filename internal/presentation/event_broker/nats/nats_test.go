package nats

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	gnats "github.com/nats-io/nats.go"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"user-service/config"
	user "user-service/internal/domain"
	eventbroker "user-service/internal/presentation/event_broker"
)

type erringRegistrationSagaMessageMapper struct{}

func (m erringRegistrationSagaMessageMapper) ToCreateFailureResultPayload(createUserCommand, string) ([]byte, error) {
	return nil, errors.New("map create failure")
}

func (m erringRegistrationSagaMessageMapper) ToCreateSuccessResultPayload(createUserCommand) ([]byte, error) {
	return nil, errors.New("map create success")
}

func (m erringRegistrationSagaMessageMapper) ToDeleteFailureResultPayload(deleteUserCommand, string) ([]byte, error) {
	return nil, errors.New("map delete failure")
}

func (m erringRegistrationSagaMessageMapper) ToDeleteSuccessResultPayload(deleteUserCommand) ([]byte, error) {
	return nil, errors.New("map delete success")
}

type stubPullConsumerValidator struct {
	err error
}

func (v stubPullConsumerValidator) Validate(config.PullConsumerConfig) error {
	return v.err
}

type stubPullConsumerRuntimeFactory struct {
	runtime PullConsumerRuntime
	err     error
}

func (f stubPullConsumerRuntimeFactory) Create(
	*gnats.Conn,
	logging.Logger,
	config.PullConsumerConfig,
	eventbroker.MessageHandler,
) (PullConsumerRuntime, error) {
	return f.runtime, f.err
}

type stubPullConsumerRuntime struct {
	started chan struct{}
}

func (r stubPullConsumerRuntime) Start(context.Context) {
	close(r.started)
}

type fakeNATSConn struct {
	publishErr   error
	subscribeErr error
	flushErr     error
	closed       bool
	handler      gnats.MsgHandler
}

func (c *fakeNATSConn) Publish(string, []byte) error {
	return c.publishErr
}

func (c *fakeNATSConn) Subscribe(_ string, handler gnats.MsgHandler) (*gnats.Subscription, error) {
	if c.subscribeErr != nil {
		return nil, c.subscribeErr
	}
	c.handler = handler
	return &gnats.Subscription{}, nil
}

func (c *fakeNATSConn) FlushWithContext(context.Context) error {
	return c.flushErr
}

func (c *fakeNATSConn) Close() {
	c.closed = true
}

func TestNATS(t *testing.T) {
	t.Helper()
	RegisterFailHandler(Fail)
	RunSpecs(t, "NATS Suite")
}

var _ = Describe("DomainFailureReasonResolver", func() {
	var resolver FailureReasonResolver

	BeforeEach(func() {
		resolver = NewDomainFailureReasonResolver()
	})

	Describe("CreateUserFailureReason", func() {
		It("maps known create-user errors", func() {
			Expect(resolver.CreateUserFailureReason(user.ErrInvalidUserID)).To(Equal("invalid user_id"))
			Expect(resolver.CreateUserFailureReason(user.ErrUserIDAlreadyTaken)).To(Equal("user_id already taken"))
			Expect(resolver.CreateUserFailureReason(user.ErrUsernameAlreadyTaken)).To(Equal("username already taken"))
		})

		It("falls back for unknown create-user errors", func() {
			Expect(resolver.CreateUserFailureReason(errors.New("boom"))).To(Equal("failed to create user"))
		})
	})

	Describe("DeleteUserFailureReason", func() {
		It("maps known delete-user errors", func() {
			Expect(resolver.DeleteUserFailureReason(user.ErrInvalidUserID)).To(Equal("invalid user_id"))
			Expect(resolver.DeleteUserFailureReason(user.ErrUserNotFound)).To(Equal("user not found"))
		})

		It("falls back for unknown delete-user errors", func() {
			Expect(resolver.DeleteUserFailureReason(errors.New("boom"))).To(Equal("failed to delete user"))
		})
	})
})

var _ = Describe("registrationSagaMessageMapper", func() {
	var mapper RegistrationSagaMessageMapper

	BeforeEach(func() {
		mapper = newRegistrationSagaMessageMapper()
	})

	It("builds a create failure payload", func() {
		payload, err := mapper.ToCreateFailureResultPayload(createUserCommand{
			SessionID: "session-1",
			UserID:    "user-1",
			SagaID:    "saga-1",
		}, "username already taken")

		Expect(err).NotTo(HaveOccurred())

		var result createUserResult
		Expect(json.Unmarshal(payload, &result)).To(Succeed())
		Expect(result.SessionID).To(Equal("session-1"))
		Expect(result.UserID).To(Equal("user-1"))
		Expect(result.SagaID).To(Equal("saga-1"))
		Expect(result.Status).To(Equal("failed"))
		Expect(result.Error).To(Equal("username already taken"))
		Expect(result.Timestamp).NotTo(BeEmpty())
	})

	It("builds a create success payload", func() {
		payload, err := mapper.ToCreateSuccessResultPayload(createUserCommand{
			SessionID: "session-1",
			UserID:    "user-1",
			SagaID:    "saga-1",
		})

		Expect(err).NotTo(HaveOccurred())

		var result createUserResult
		Expect(json.Unmarshal(payload, &result)).To(Succeed())
		Expect(result.Status).To(Equal("success"))
		Expect(result.Error).To(BeEmpty())
	})

	It("builds a delete failure payload", func() {
		payload, err := mapper.ToDeleteFailureResultPayload(deleteUserCommand{
			SessionID: "session-1",
			UserID:    "user-1",
			SagaID:    "saga-1",
		}, "user not found")

		Expect(err).NotTo(HaveOccurred())

		var result deleteUserResult
		Expect(json.Unmarshal(payload, &result)).To(Succeed())
		Expect(result.Status).To(Equal("failed"))
		Expect(result.Error).To(Equal("user not found"))
	})

	It("builds a delete success payload", func() {
		payload, err := mapper.ToDeleteSuccessResultPayload(deleteUserCommand{
			SessionID: "session-1",
			UserID:    "user-1",
			SagaID:    "saga-1",
		})

		Expect(err).NotTo(HaveOccurred())

		var result deleteUserResult
		Expect(json.Unmarshal(payload, &result)).To(Succeed())
		Expect(result.Status).To(Equal("success"))
		Expect(result.Error).To(BeEmpty())
	})

	It("maps marshal failures to result errors", func() {
		previousMarshal := jsonMarshal
		defer func() { jsonMarshal = previousMarshal }()

		jsonMarshal = func(any) ([]byte, error) {
			return nil, errors.New("marshal failed")
		}

		payload, err := mapper.ToCreateFailureResultPayload(createUserCommand{UserID: "user-1"}, "failed")
		Expect(payload).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("marshal create user result")))

		payload, err = mapper.ToCreateSuccessResultPayload(createUserCommand{UserID: "user-1"})
		Expect(payload).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("marshal create user result")))

		payload, err = mapper.ToDeleteFailureResultPayload(deleteUserCommand{UserID: "user-1"}, "failed")
		Expect(payload).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("marshal delete user result")))

		payload, err = mapper.ToDeleteSuccessResultPayload(deleteUserCommand{UserID: "user-1"})
		Expect(payload).To(BeNil())
		Expect(err).To(MatchError(ContainSubstring("marshal delete user result")))
	})
})

var _ = Describe("BuildAdaptiveConfig", func() {
	It("projects saga adaptive settings from config", func() {
		cfg := config.NATSConfig{
			SagaAdaptiveEnabled:         true,
			SagaAdaptiveCheckInterval:   2 * time.Second,
			SagaAdaptiveMediumPending:   20,
			SagaAdaptiveHighPending:     50,
			SagaAdaptiveLowBatchSize:    10,
			SagaAdaptiveLowMaxWait:      time.Second,
			SagaAdaptiveMediumBatchSize: 25,
			SagaAdaptiveMediumMaxWait:   500 * time.Millisecond,
			SagaAdaptiveHighBatchSize:   100,
			SagaAdaptiveHighMaxWait:     100 * time.Millisecond,
		}

		Expect(BuildAdaptiveConfig(cfg)).To(Equal(config.PullAdaptiveConfig{
			Enabled:         true,
			CheckInterval:   2 * time.Second,
			MediumPending:   20,
			HighPending:     50,
			LowBatchSize:    10,
			LowMaxWait:      time.Second,
			MediumBatchSize: 25,
			MediumMaxWait:   500 * time.Millisecond,
			HighBatchSize:   100,
			HighMaxWait:     100 * time.Millisecond,
		}))
	})
})

var _ = Describe("pullConsumerConfigValidator", func() {
	It("constructs a validator and accepts a valid config", func() {
		validator := newPullConsumerConfigValidator()

		err := validator.Validate(config.PullConsumerConfig{
			Stream:     "SAGA_USER_COMMANDS",
			Subject:    "saga.user.create",
			Durable:    "user_create",
			BatchSize:  5,
			MaxWait:    time.Second,
			Workers:    2,
			QueueSize:  10,
			AckWait:    30 * time.Second,
			MaxDeliver: 3,
			Adaptive: config.PullAdaptiveConfig{
				Enabled:         true,
				CheckInterval:   time.Second,
				MediumPending:   10,
				HighPending:     20,
				LowBatchSize:    5,
				LowMaxWait:      time.Second,
				MediumBatchSize: 10,
				MediumMaxWait:   500 * time.Millisecond,
				HighBatchSize:   20,
				HighMaxWait:     250 * time.Millisecond,
			},
		})

		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects invalid configs in validation order", func() {
		validator := newPullConsumerConfigValidator()
		valid := config.PullConsumerConfig{
			Stream:     "S",
			Subject:    "subject",
			Durable:    "durable",
			BatchSize:  1,
			MaxWait:    time.Second,
			Workers:    1,
			QueueSize:  1,
			AckWait:    time.Second,
			MaxDeliver: 1,
		}

		cfg := valid
		cfg.Stream = ""
		Expect(validator.Validate(cfg)).To(MatchError(ErrEmptyStreamName))

		cfg = valid
		cfg.Subject = ""
		Expect(validator.Validate(cfg)).To(MatchError(ErrEmptySubject))

		cfg = valid
		cfg.Durable = ""
		Expect(validator.Validate(cfg)).To(MatchError(ErrEmptyDurableName))

		cfg = valid
		cfg.BatchSize = 0
		Expect(validator.Validate(cfg)).To(MatchError(ErrInvalidBatchSize))

		cfg = valid
		cfg.MaxWait = 0
		Expect(validator.Validate(cfg)).To(MatchError(ErrInvalidMaxWait))

		cfg = valid
		cfg.Workers = 0
		Expect(validator.Validate(cfg)).To(MatchError(ErrInvalidWorkerCount))

		cfg = valid
		cfg.QueueSize = 0
		Expect(validator.Validate(cfg)).To(MatchError(ErrInvalidQueueSize))

		cfg = valid
		cfg.AckWait = 0
		Expect(validator.Validate(cfg)).To(MatchError(ErrInvalidAckWait))

		cfg = valid
		cfg.MaxDeliver = 0
		Expect(validator.Validate(cfg)).To(MatchError(ErrInvalidMaxDeliver))

		cfg = valid
		cfg.Adaptive.Enabled = true
		cfg.Adaptive.CheckInterval = 0
		Expect(validator.Validate(cfg)).To(MatchError(ErrInvalidAdaptiveCheckInterval))

		cfg = valid
		cfg.Adaptive = config.PullAdaptiveConfig{Enabled: true, CheckInterval: time.Second, MediumPending: 2, HighPending: 2}
		Expect(validator.Validate(cfg)).To(MatchError(ErrInvalidAdaptiveThresholds))

		cfg = valid
		cfg.Adaptive = config.PullAdaptiveConfig{
			Enabled:       true,
			CheckInterval: time.Second,
			MediumPending: 1,
			HighPending:   2,
			LowBatchSize:  0,
			LowMaxWait:    time.Second,
		}
		Expect(validator.Validate(cfg)).To(MatchError(ErrInvalidAdaptivePlan))
	})
})

var _ = Describe("natsBroker unit", func() {
	var logger logging.Logger

	BeforeEach(func() {
		var err error
		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	It("runs pull consumers through injected validator and runtime factory", func() {
		started := make(chan struct{})
		broker := &natsBroker{
			log:            logger,
			validator:      stubPullConsumerValidator{},
			runtimeFactory: stubPullConsumerRuntimeFactory{runtime: stubPullConsumerRuntime{started: started}},
		}

		cfg := config.PullConsumerConfig{
			Stream:     "S",
			Subject:    "subject",
			Durable:    "durable",
			BatchSize:  1,
			MaxWait:    time.Second,
			Workers:    1,
			QueueSize:  1,
			AckWait:    time.Second,
			MaxDeliver: 1,
			Adaptive: config.PullAdaptiveConfig{
				Enabled:       true,
				MediumPending: 1,
				HighPending:   2,
			},
		}

		Expect(broker.RunPullConsumer(context.Background(), cfg, func(context.Context, string, []byte) error { return nil })).To(Succeed())
		Eventually(started).Should(BeClosed())
	})

	It("validates construction and wraps connect failures", func() {
		broker, err := NewBroker(config.NATSConfig{}, logger)
		Expect(broker).To(BeNil())
		Expect(err).To(MatchError(ErrEmptyNATSURL))

		broker, err = NewBroker(config.NATSConfig{URL: "nats://127.0.0.1:1", User: "user", Password: "pass"}, nil)
		Expect(broker).To(BeNil())
		Expect(err).To(MatchError(ErrNilLogger))

		broker, err = NewBroker(config.NATSConfig{URL: "nats://127.0.0.1:1", User: "user", Password: "pass"}, logger)
		Expect(broker).To(BeNil())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("connect to nats"))
	})

	It("publishes and flushes messages", func() {
		conn := &fakeNATSConn{}
		broker := &natsBroker{nc: conn, log: logger}

		Expect(broker.Publish(context.Background(), "subject", []byte("payload"))).To(Succeed())

		conn.publishErr = errors.New("publish failed")
		Expect(broker.Publish(context.Background(), "subject", []byte("payload"))).To(MatchError(ContainSubstring("publish to nats")))

		conn.publishErr = nil
		conn.flushErr = errors.New("flush failed")
		Expect(broker.Publish(context.Background(), "subject", []byte("payload"))).To(MatchError(ContainSubstring("flush nats publisher")))
	})

	It("subscribes and dispatches messages", func() {
		conn := &fakeNATSConn{}
		broker := &natsBroker{nc: conn, log: logger}

		handled := make(chan []byte, 1)
		Expect(broker.Subscribe(context.Background(), "subject", func(_ context.Context, subject string, payload []byte) error {
			Expect(subject).To(Equal("subject"))
			handled <- payload
			return nil
		})).To(Succeed())
		conn.handler(&gnats.Msg{Subject: "subject", Data: []byte("payload")})
		Eventually(handled).Should(Receive(Equal([]byte("payload"))))

		Expect(broker.Subscribe(context.Background(), "subject", func(context.Context, string, []byte) error {
			return errors.New("handler failed")
		})).To(Succeed())
		Expect(func() { conn.handler(&gnats.Msg{Subject: "subject", Data: []byte("bad")}) }).NotTo(Panic())

		conn.subscribeErr = errors.New("subscribe failed")
		Expect(broker.Subscribe(context.Background(), "subject", nil)).To(MatchError(ContainSubstring("subscribe to nats")))

		conn.subscribeErr = nil
		conn.flushErr = errors.New("flush failed")
		Expect(broker.Subscribe(context.Background(), "subject", nil)).To(MatchError(ContainSubstring("flush nats publisher")))
	})

	It("returns validator and runtime factory errors", func() {
		broker := &natsBroker{log: logger, validator: stubPullConsumerValidator{err: errors.New("invalid")}}
		Expect(broker.RunPullConsumer(context.Background(), config.PullConsumerConfig{}, nil)).To(MatchError("invalid"))

		broker = &natsBroker{
			log:            logger,
			validator:      stubPullConsumerValidator{},
			runtimeFactory: stubPullConsumerRuntimeFactory{err: errors.New("runtime failed")},
		}
		Expect(broker.RunPullConsumer(context.Background(), config.PullConsumerConfig{}, nil)).To(MatchError("runtime failed"))
	})

	It("closes safely without a connection and resolves pull plans", func() {
		broker := &natsBroker{log: logger}
		Expect(func() { broker.Close() }).NotTo(Panic())

		conn := &fakeNATSConn{}
		broker.nc = conn
		broker.Close()
		Expect(conn.closed).To(BeTrue())

		cfg := config.PullConsumerConfig{
			BatchSize: 5,
			MaxWait:   time.Second,
			Adaptive: config.PullAdaptiveConfig{
				Enabled:         true,
				MediumPending:   10,
				HighPending:     20,
				LowBatchSize:    2,
				LowMaxWait:      40 * time.Millisecond,
				MediumBatchSize: 4,
				MediumMaxWait:   20 * time.Millisecond,
				HighBatchSize:   8,
				HighMaxWait:     10 * time.Millisecond,
			},
		}

		tier, batch, waitFor := ResolvePullPlan(cfg, 20)
		Expect(tier).To(Equal("high"))
		Expect(batch).To(Equal(8))
		Expect(waitFor).To(Equal(10 * time.Millisecond))

		tier, batch, waitFor = ResolvePullPlan(cfg, 10)
		Expect(tier).To(Equal("medium"))
		Expect(batch).To(Equal(4))
		Expect(waitFor).To(Equal(20 * time.Millisecond))

		tier, batch, waitFor = ResolvePullPlan(cfg, 1)
		Expect(tier).To(Equal("low"))
		Expect(batch).To(Equal(2))
		Expect(waitFor).To(Equal(40 * time.Millisecond))

		cfg.Adaptive.Enabled = false
		tier, batch, waitFor = ResolvePullPlan(cfg, 100)
		Expect(tier).To(Equal("base"))
		Expect(batch).To(Equal(5))
		Expect(waitFor).To(Equal(time.Second))
	})

	It("flushes with caller deadlines and synthetic deadlines", func() {
		conn := &fakeNATSConn{}
		Expect(Flush(context.Background(), conn)).To(Succeed())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		Expect(Flush(ctx, conn)).To(Succeed())

		conn.flushErr = errors.New("flush failed")
		Expect(Flush(ctx, conn)).To(MatchError("flush failed"))
	})
})

var _ = Describe("pullConsumerRuntime unit", func() {
	var logger logging.Logger

	BeforeEach(func() {
		var err error
		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	It("constructs the default factory", func() {
		Expect(newPullConsumerRuntimeFactory()).NotTo(BeNil())
	})

	It("starts workers and stops immediately on cancelled context", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		runtime := &pullConsumerRuntime{
			log:     logger,
			cfg:     config.PullConsumerConfig{Workers: 1, BatchSize: 1, MaxWait: time.Millisecond, Subject: "subject", Durable: "durable"},
			handler: func(context.Context, string, []byte) error { return nil },
			jobs:    make(chan *gnats.Msg, 1),
		}

		Expect(func() { runtime.Start(ctx) }).NotTo(Panic())
		Eventually(runtime.jobs).Should(BeClosed())
	})

	It("handles worker success, handler failure, closed queues, and cancellation", func() {
		jobs := make(chan *gnats.Msg, 2)
		jobs <- &gnats.Msg{Subject: "subject", Data: []byte("ok")}
		close(jobs)
		var handled int
		runtime := &pullConsumerRuntime{
			log:  logger,
			jobs: jobs,
			handler: func(context.Context, string, []byte) error {
				handled++
				return nil
			},
		}
		var wg sync.WaitGroup
		wg.Add(1)
		runtime.runWorker(context.Background(), &wg, 1)
		Expect(handled).To(Equal(1))

		jobs = make(chan *gnats.Msg, 1)
		jobs <- &gnats.Msg{Subject: "subject", Data: []byte("bad")}
		close(jobs)
		runtime.jobs = jobs
		runtime.handler = func(context.Context, string, []byte) error { return errors.New("handler failed") }
		wg.Add(1)
		runtime.runWorker(context.Background(), &wg, 1)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		runtime.jobs = make(chan *gnats.Msg)
		wg.Add(1)
		runtime.runWorker(ctx, &wg, 1)
	})

	It("exits fetcher on cancelled context", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		runtime := &pullConsumerRuntime{
			log:  logger,
			cfg:  config.PullConsumerConfig{BatchSize: 1, MaxWait: time.Millisecond, Subject: "subject", Durable: "durable"},
			jobs: make(chan *gnats.Msg, 1),
		}

		var wg sync.WaitGroup
		runtime.runFetcher(ctx, &wg)
		Expect(runtime.jobs).To(BeClosed())
	})
})

var _ = Describe("error wrappers", func() {
	It("preserves wrapped causes", func() {
		cause := errors.New("boom")

		Expect(WrapMarshalCreateUserResultError(cause)).To(MatchError(ContainSubstring("marshal create user result")))
		Expect(WrapMarshalDeleteUserResultError(cause)).To(MatchError(ContainSubstring("marshal delete user result")))
	})
})

var _ = Describe("RegistrationSagaSubscriber", func() {
	var (
		ctrl     *gomock.Controller
		broker   *MockEventBroker
		service  *MockUserService
		logger   logging.Logger
		resolver FailureReasonResolver
		cfg      config.NATSConfig
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		broker = NewMockEventBroker(ctrl)
		service = NewMockUserService(ctrl)
		resolver = NewDomainFailureReasonResolver()
		cfg = config.NATSConfig{
			SagaCommandsStream:          "SAGA_USER_COMMANDS",
			SagaCreateUserSubject:       "saga.user.create",
			SagaDeleteUserSubject:       "saga.user.delete",
			SagaCreateUserResultSubject: "saga.user.create.result",
			SagaDeleteUserResultSubject: "saga.user.delete.result",
			SagaCreateUserDurable:       "user_create",
			SagaDeleteUserDurable:       "user_delete",
			SagaBatchSize:               10,
			SagaMaxWait:                 250 * time.Millisecond,
			SagaWorkers:                 2,
			SagaQueueSize:               20,
			SagaAckWait:                 time.Second,
			SagaMaxDeliver:              3,
			SagaAdaptiveEnabled:         true,
			SagaAdaptiveCheckInterval:   2 * time.Second,
			SagaAdaptiveMediumPending:   10,
			SagaAdaptiveHighPending:     20,
			SagaAdaptiveLowBatchSize:    5,
			SagaAdaptiveLowMaxWait:      time.Second,
			SagaAdaptiveMediumBatchSize: 10,
			SagaAdaptiveMediumMaxWait:   500 * time.Millisecond,
			SagaAdaptiveHighBatchSize:   20,
			SagaAdaptiveHighMaxWait:     200 * time.Millisecond,
		}

		var err error
		logger, err = logging.New("user-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Describe("NewRegistrationSagaSubscriber", func() {
		It("validates nil collaborators", func() {
			sub, err := NewRegistrationSagaSubscriber(nil, service, cfg, resolver, logger)
			Expect(sub).To(BeNil())
			Expect(err).To(MatchError(ErrNilBroker))

			sub, err = NewRegistrationSagaSubscriber(broker, nil, cfg, resolver, logger)
			Expect(sub).To(BeNil())
			Expect(err).To(MatchError(ErrNilUserService))

			sub, err = NewRegistrationSagaSubscriber(broker, service, cfg, nil, logger)
			Expect(sub).To(BeNil())
			Expect(err).To(MatchError(ErrNilFailureReasonResolver))

			sub, err = NewRegistrationSagaSubscriber(broker, service, cfg, resolver, nil)
			Expect(sub).To(BeNil())
			Expect(err).To(MatchError(ErrNilLogger))
		})
	})

	Describe("Subscribe", func() {
		It("registers the create and delete consumers", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			var createCfg config.PullConsumerConfig
			var deleteCfg config.PullConsumerConfig

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, _ eventbroker.MessageHandler) error {
					createCfg = pcfg
					return nil
				})

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, _ eventbroker.MessageHandler) error {
					deleteCfg = pcfg
					return nil
				})

			Expect(sub.Subscribe(context.Background())).To(Succeed())
			Expect(createCfg.Subject).To(Equal(cfg.SagaCreateUserSubject))
			Expect(createCfg.Durable).To(Equal(cfg.SagaCreateUserDurable))
			Expect(createCfg.Adaptive).To(Equal(BuildAdaptiveConfig(cfg)))
			Expect(deleteCfg.Subject).To(Equal(cfg.SagaDeleteUserSubject))
			Expect(deleteCfg.Durable).To(Equal(cfg.SagaDeleteUserDurable))
			Expect(deleteCfg.Adaptive).To(Equal(BuildAdaptiveConfig(cfg)))
		})

		It("builds pull-consumer config from subscriber settings", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			pullCfg := sub.(*registrationSagaSubscriber).buildPullConsumerConfig(cfg.SagaCreateUserSubject, cfg.SagaCreateUserDurable)

			Expect(pullCfg).To(Equal(config.PullConsumerConfig{
				Stream:     cfg.SagaCommandsStream,
				Subject:    cfg.SagaCreateUserSubject,
				Durable:    cfg.SagaCreateUserDurable,
				BatchSize:  cfg.SagaBatchSize,
				MaxWait:    cfg.SagaMaxWait,
				Workers:    cfg.SagaWorkers,
				QueueSize:  cfg.SagaQueueSize,
				AckWait:    cfg.SagaAckWait,
				MaxDeliver: cfg.SagaMaxDeliver,
				Adaptive:   BuildAdaptiveConfig(cfg),
			}))
		})

		It("returns the create-consumer registration error", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(errors.New("create consumer failed"))

			Expect(sub.Subscribe(context.Background())).To(MatchError("create consumer failed"))
		})

		It("returns the delete-consumer registration error", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil)
			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(errors.New("delete consumer failed"))

			Expect(sub.Subscribe(context.Background())).To(MatchError("delete consumer failed"))
		})

		It("handles a create-user success command", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			var createHandler eventbroker.MessageHandler

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
					if pcfg.Subject == cfg.SagaCreateUserSubject {
						createHandler = handler
					}
					return nil
				})
			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil)

			Expect(sub.Subscribe(context.Background())).To(Succeed())
			Expect(createHandler).NotTo(BeNil())

			service.EXPECT().
				CreateUser(gomock.Any(), "user-1", "alex", "Alex", "Doe").
				Return(&user.User{ID: "user-1", Username: "alex"}, nil)

			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaCreateUserResultSubject, gomock.Any()).
				DoAndReturn(func(_ context.Context, subject string, payload []byte) error {
					Expect(subject).To(Equal(cfg.SagaCreateUserResultSubject))
					var result createUserResult
					Expect(json.Unmarshal(payload, &result)).To(Succeed())
					Expect(result.UserID).To(Equal("user-1"))
					Expect(result.Status).To(Equal("success"))
					Expect(result.Error).To(BeEmpty())
					return nil
				})

			payload, err := json.Marshal(createUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
				Username:  "alex",
				FirstName: "Alex",
				LastName:  "Doe",
				SagaID:    "saga-1",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(createHandler(context.Background(), cfg.SagaCreateUserSubject, payload)).To(Succeed())
		})

		It("handles create-user commands directly", func() {
			subAny, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())
			sub := subAny.(*registrationSagaSubscriber)

			service.EXPECT().
				CreateUser(gomock.Any(), "user-1", "alex", "Alex", "Doe").
				Return(&user.User{ID: "user-1", Username: "alex"}, nil)
			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaCreateUserResultSubject, gomock.Any()).
				DoAndReturn(func(_ context.Context, subject string, payload []byte) error {
					Expect(subject).To(Equal(cfg.SagaCreateUserResultSubject))
					var result createUserResult
					Expect(json.Unmarshal(payload, &result)).To(Succeed())
					Expect(result.Status).To(Equal("success"))
					return nil
				})

			payload, err := json.Marshal(createUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
				Username:  "alex",
				FirstName: "Alex",
				LastName:  "Doe",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(sub.handleCreateUserCommand(context.Background(), cfg.SagaCreateUserSubject, payload)).To(Succeed())
		})

		It("handles a create-user failure command", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			var createHandler eventbroker.MessageHandler

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
					if pcfg.Subject == cfg.SagaCreateUserSubject {
						createHandler = handler
					}
					return nil
				})
			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil)

			Expect(sub.Subscribe(context.Background())).To(Succeed())
			Expect(createHandler).NotTo(BeNil())

			service.EXPECT().
				CreateUser(gomock.Any(), "user-1", "alex", "Alex", "Doe").
				Return(nil, user.ErrUsernameAlreadyTaken)

			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaCreateUserResultSubject, gomock.Any()).
				DoAndReturn(func(_ context.Context, subject string, payload []byte) error {
					Expect(subject).To(Equal(cfg.SagaCreateUserResultSubject))
					var result createUserResult
					Expect(json.Unmarshal(payload, &result)).To(Succeed())
					Expect(result.Status).To(Equal("failed"))
					Expect(result.Error).To(Equal("username already taken"))
					return nil
				})

			payload, err := json.Marshal(createUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
				Username:  "alex",
				FirstName: "Alex",
				LastName:  "Doe",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(createHandler(context.Background(), cfg.SagaCreateUserSubject, payload)).To(Succeed())
		})

		It("handles create-user failure and malformed commands directly", func() {
			subAny, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())
			sub := subAny.(*registrationSagaSubscriber)

			service.EXPECT().
				CreateUser(gomock.Any(), "user-1", "alex", "Alex", "Doe").
				Return(nil, user.ErrUsernameAlreadyTaken)
			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaCreateUserResultSubject, gomock.Any()).
				DoAndReturn(func(_ context.Context, subject string, payload []byte) error {
					Expect(subject).To(Equal(cfg.SagaCreateUserResultSubject))
					var result createUserResult
					Expect(json.Unmarshal(payload, &result)).To(Succeed())
					Expect(result.Status).To(Equal("failed"))
					Expect(result.Error).To(Equal("username already taken"))
					return nil
				})

			payload, err := json.Marshal(createUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
				Username:  "alex",
				FirstName: "Alex",
				LastName:  "Doe",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(sub.handleCreateUserCommand(context.Background(), cfg.SagaCreateUserSubject, payload)).To(Succeed())
			Expect(sub.handleCreateUserCommand(context.Background(), cfg.SagaCreateUserSubject, []byte("{"))).
				To(MatchError(ContainSubstring("unmarshal create user command")))
		})

		It("propagates create-user result publish failures", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			var createHandler eventbroker.MessageHandler

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
					if pcfg.Subject == cfg.SagaCreateUserSubject {
						createHandler = handler
					}
					return nil
				})
			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil)

			Expect(sub.Subscribe(context.Background())).To(Succeed())
			Expect(createHandler).NotTo(BeNil())

			service.EXPECT().
				CreateUser(gomock.Any(), "user-1", "alex", "Alex", "Doe").
				Return(&user.User{ID: "user-1", Username: "alex"}, nil)

			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaCreateUserResultSubject, gomock.Any()).
				Return(errors.New("publish failed"))

			payload, err := json.Marshal(createUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
				Username:  "alex",
				FirstName: "Alex",
				LastName:  "Doe",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(createHandler(context.Background(), cfg.SagaCreateUserSubject, payload)).To(MatchError("publish failed"))
		})

		It("rejects malformed create-user commands", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			var createHandler eventbroker.MessageHandler

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
					if pcfg.Subject == cfg.SagaCreateUserSubject {
						createHandler = handler
					}
					return nil
				})
			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil)

			Expect(sub.Subscribe(context.Background())).To(Succeed())
			Expect(createHandler).NotTo(BeNil())

			err = createHandler(context.Background(), cfg.SagaCreateUserSubject, []byte("{"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unmarshal create user command"))
		})

		It("handles a delete-user success command", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			var deleteHandler eventbroker.MessageHandler

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil)
			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
					if pcfg.Subject == cfg.SagaDeleteUserSubject {
						deleteHandler = handler
					}
					return nil
				})

			Expect(sub.Subscribe(context.Background())).To(Succeed())
			Expect(deleteHandler).NotTo(BeNil())

			service.EXPECT().DeleteUser(gomock.Any(), "user-1").Return(nil)

			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaDeleteUserResultSubject, gomock.Any()).
				DoAndReturn(func(_ context.Context, subject string, payload []byte) error {
					Expect(subject).To(Equal(cfg.SagaDeleteUserResultSubject))
					var result deleteUserResult
					Expect(json.Unmarshal(payload, &result)).To(Succeed())
					Expect(result.UserID).To(Equal("user-1"))
					Expect(result.Status).To(Equal("success"))
					return nil
				})

			payload, err := json.Marshal(deleteUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
				SagaID:    "saga-1",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(deleteHandler(context.Background(), cfg.SagaDeleteUserSubject, payload)).To(Succeed())
		})

		It("handles delete-user commands directly", func() {
			subAny, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())
			sub := subAny.(*registrationSagaSubscriber)

			service.EXPECT().DeleteUser(gomock.Any(), "user-1").Return(nil)
			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaDeleteUserResultSubject, gomock.Any()).
				DoAndReturn(func(_ context.Context, subject string, payload []byte) error {
					Expect(subject).To(Equal(cfg.SagaDeleteUserResultSubject))
					var result deleteUserResult
					Expect(json.Unmarshal(payload, &result)).To(Succeed())
					Expect(result.Status).To(Equal("success"))
					return nil
				})

			payload, err := json.Marshal(deleteUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(sub.handleDeleteUserCommand(context.Background(), cfg.SagaDeleteUserSubject, payload)).To(Succeed())
		})

		It("returns mapper errors from publish helpers", func() {
			subAny, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())
			sub := subAny.(*registrationSagaSubscriber)
			sub.mapr = erringRegistrationSagaMessageMapper{}

			Expect(sub.publishCreateFailureResult(context.Background(), createUserCommand{}, user.ErrInvalidUserID)).
				To(MatchError("map create failure"))
			Expect(sub.publishCreateSuccessResult(context.Background(), createUserCommand{})).
				To(MatchError("map create success"))
			Expect(sub.publishDeleteFailureResult(context.Background(), deleteUserCommand{}, user.ErrInvalidUserID)).
				To(MatchError("map delete failure"))
			Expect(sub.publishDeleteSuccessResult(context.Background(), deleteUserCommand{})).
				To(MatchError("map delete success"))
		})

		It("handles a delete-user failure command", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			var deleteHandler eventbroker.MessageHandler

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil)
			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
					if pcfg.Subject == cfg.SagaDeleteUserSubject {
						deleteHandler = handler
					}
					return nil
				})

			Expect(sub.Subscribe(context.Background())).To(Succeed())
			Expect(deleteHandler).NotTo(BeNil())

			service.EXPECT().DeleteUser(gomock.Any(), "user-1").Return(user.ErrUserNotFound)

			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaDeleteUserResultSubject, gomock.Any()).
				DoAndReturn(func(_ context.Context, subject string, payload []byte) error {
					Expect(subject).To(Equal(cfg.SagaDeleteUserResultSubject))
					var result deleteUserResult
					Expect(json.Unmarshal(payload, &result)).To(Succeed())
					Expect(result.Status).To(Equal("failed"))
					Expect(result.Error).To(Equal("user not found"))
					return nil
				})

			payload, err := json.Marshal(deleteUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(deleteHandler(context.Background(), cfg.SagaDeleteUserSubject, payload)).To(Succeed())
		})

		It("handles delete-user failure and malformed commands directly", func() {
			subAny, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())
			sub := subAny.(*registrationSagaSubscriber)

			service.EXPECT().DeleteUser(gomock.Any(), "user-1").Return(user.ErrUserNotFound)
			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaDeleteUserResultSubject, gomock.Any()).
				DoAndReturn(func(_ context.Context, subject string, payload []byte) error {
					Expect(subject).To(Equal(cfg.SagaDeleteUserResultSubject))
					var result deleteUserResult
					Expect(json.Unmarshal(payload, &result)).To(Succeed())
					Expect(result.Status).To(Equal("failed"))
					Expect(result.Error).To(Equal("user not found"))
					return nil
				})

			payload, err := json.Marshal(deleteUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(sub.handleDeleteUserCommand(context.Background(), cfg.SagaDeleteUserSubject, payload)).To(Succeed())
			Expect(sub.handleDeleteUserCommand(context.Background(), cfg.SagaDeleteUserSubject, []byte("{"))).
				To(MatchError(ContainSubstring("unmarshal delete user command")))
		})

		It("propagates delete-user result publish failures", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			var deleteHandler eventbroker.MessageHandler

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil)
			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
					if pcfg.Subject == cfg.SagaDeleteUserSubject {
						deleteHandler = handler
					}
					return nil
				})

			Expect(sub.Subscribe(context.Background())).To(Succeed())
			Expect(deleteHandler).NotTo(BeNil())

			service.EXPECT().DeleteUser(gomock.Any(), "user-1").Return(user.ErrUserNotFound)

			broker.EXPECT().
				Publish(gomock.Any(), cfg.SagaDeleteUserResultSubject, gomock.Any()).
				Return(errors.New("publish failed"))

			payload, err := json.Marshal(deleteUserCommand{
				SessionID: "session-1",
				UserID:    "user-1",
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(deleteHandler(context.Background(), cfg.SagaDeleteUserSubject, payload)).To(MatchError("publish failed"))
		})

		It("rejects malformed delete-user commands", func() {
			sub, err := NewRegistrationSagaSubscriber(broker, service, cfg, resolver, logger)
			Expect(err).NotTo(HaveOccurred())

			var deleteHandler eventbroker.MessageHandler

			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil)
			broker.EXPECT().
				RunPullConsumer(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, pcfg config.PullConsumerConfig, handler eventbroker.MessageHandler) error {
					if pcfg.Subject == cfg.SagaDeleteUserSubject {
						deleteHandler = handler
					}
					return nil
				})

			Expect(sub.Subscribe(context.Background())).To(Succeed())
			Expect(deleteHandler).NotTo(BeNil())

			err = deleteHandler(context.Background(), cfg.SagaDeleteUserSubject, []byte("{"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unmarshal delete user command"))
		})
	})
})
