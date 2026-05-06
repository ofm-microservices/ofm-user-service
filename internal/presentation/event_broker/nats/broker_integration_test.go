package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"user-service/config"
)

var (
	brokerSuiteContainer testcontainers.Container
	brokerSuiteBaseCfg   config.NATSConfig
	brokerSuiteLogger    logging.Logger
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
	brokerSuiteLogger, err = logging.New("user-service", "test", "debug")
	Expect(err).NotTo(HaveOccurred())

	brokerSuiteContainer, brokerSuiteBaseCfg = startBrokerNATSContainer(ctx)
})

var _ = AfterSuite(func() {
	if brokerSuiteContainer != nil {
		Expect(brokerSuiteContainer.Terminate(context.Background())).To(Succeed())
	}
})

var _ = Describe("natsBroker integration", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		cfg    config.NATSConfig
	)

	BeforeEach(func() {
		if brokerSuiteContainer == nil {
			Skip("Docker is not available for the NATS integration suite")
		}
		ctx, cancel = context.WithTimeout(context.Background(), 45*time.Second)
		cfg = uniqueBrokerConfig(brokerSuiteBaseCfg)

		Expect(ensureBrokerStreams(cfg)).To(Succeed())
	})

	AfterEach(func() {
		cancel()
	})

	It("validates broker construction", func() {
		broker, err := NewBroker(config.NATSConfig{}, brokerSuiteLogger)
		Expect(broker).To(BeNil())
		Expect(err).To(MatchError(ErrEmptyNATSURL))

		broker, err = NewBroker(cfg, nil)
		Expect(broker).To(BeNil())
		Expect(err).To(MatchError(ErrNilLogger))
	})

	It("publishes messages to core nats subjects", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		sub, err := rawConn.SubscribeSync(cfg.UserCreatedSubject)
		Expect(err).NotTo(HaveOccurred())
		Expect(rawConn.Flush()).To(Succeed())

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		defer brokerAny.Close()

		Expect(brokerAny.Publish(ctx, cfg.UserCreatedSubject, []byte("payload"))).To(Succeed())

		msg, err := sub.NextMsg(5 * time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(msg.Data)).To(Equal("payload"))
	})

	It("flushes with a synthetic timeout when the caller provides no deadline", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		Expect(Flush(context.Background(), rawConn)).To(Succeed())
	})

	It("subscribes and dispatches core nats messages", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		defer brokerAny.Close()

		received := make(chan []byte, 1)
		Expect(brokerAny.Subscribe(ctx, cfg.UserCreatedSubject, func(_ context.Context, subject string, payload []byte) error {
			Expect(subject).To(Equal(cfg.UserCreatedSubject))
			received <- payload
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.UserCreatedSubject, []byte("payload"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(received).Should(Receive(Equal([]byte("payload"))))
	})

	It("continues after a subscriber handler error", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		defer brokerAny.Close()

		received := make(chan string, 1)
		Expect(brokerAny.Subscribe(ctx, cfg.UserCreatedSubject, func(_ context.Context, _ string, payload []byte) error {
			if string(payload) == "bad" {
				return errors.New("boom")
			}
			received <- string(payload)
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.UserCreatedSubject, []byte("bad"))).To(Succeed())
		Expect(rawConn.Publish(cfg.UserCreatedSubject, []byte("good"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(received).Should(Receive(Equal("good")))
	})

	It("runs a pull consumer against jetstream", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		defer concrete.Close()

		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()

		var handled atomic.Int32
		Expect(concrete.RunPullConsumer(runCtx, config.PullConsumerConfig{
			Stream:     cfg.SagaCommandsStream,
			Subject:    cfg.SagaCreateUserSubject,
			Durable:    cfg.SagaCreateUserDurable,
			BatchSize:  2,
			MaxWait:    50 * time.Millisecond,
			Workers:    1,
			QueueSize:  4,
			AckWait:    2 * time.Second,
			MaxDeliver: 3,
			Adaptive: config.PullAdaptiveConfig{
				Enabled:         true,
				CheckInterval:   time.Millisecond,
				MediumPending:   1,
				HighPending:     2,
				LowBatchSize:    1,
				LowMaxWait:      20 * time.Millisecond,
				MediumBatchSize: 2,
				MediumMaxWait:   10 * time.Millisecond,
				HighBatchSize:   3,
				HighMaxWait:     5 * time.Millisecond,
			},
		}, func(_ context.Context, subject string, payload []byte) error {
			Expect(subject).To(Equal(cfg.SagaCreateUserSubject))
			Expect(payload).NotTo(BeEmpty())
			handled.Add(1)
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.SagaCreateUserSubject, []byte("one"))).To(Succeed())
		Expect(rawConn.Publish(cfg.SagaCreateUserSubject, []byte("two"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(func() int32 { return handled.Load() }).Should(BeNumerically(">=", 2))
		runCancel()
	})

	It("falls back to default validator and runtime factory when broker helpers are nil", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		concrete.validator = nil
		concrete.runtimeFactory = nil
		defer concrete.Close()

		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()

		var handled atomic.Int32
		Expect(concrete.RunPullConsumer(runCtx, config.PullConsumerConfig{
			Stream:     cfg.SagaCommandsStream,
			Subject:    cfg.SagaCreateUserSubject,
			Durable:    cfg.SagaCreateUserDurable,
			BatchSize:  1,
			MaxWait:    50 * time.Millisecond,
			Workers:    1,
			QueueSize:  2,
			AckWait:    time.Second,
			MaxDeliver: 2,
		}, func(_ context.Context, _ string, _ []byte) error {
			handled.Add(1)
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.SagaCreateUserSubject, []byte("one"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(func() int32 { return handled.Load() }).Should(Equal(int32(1)))
	})

	It("validates pull consumer config before touching nats", func() {
		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		defer concrete.Close()

		err = concrete.RunPullConsumer(ctx, config.PullConsumerConfig{}, func(context.Context, string, []byte) error { return nil })
		Expect(err).To(MatchError(ErrEmptyStreamName))

		err = concrete.RunPullConsumer(ctx, config.PullConsumerConfig{Stream: "stream"}, func(context.Context, string, []byte) error { return nil })
		Expect(err).To(MatchError(ErrEmptySubject))
	})

	It("validates the remaining pull-consumer config branches", func() {
		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		defer concrete.Close()

		base := config.PullConsumerConfig{
			Stream:     "stream",
			Subject:    "subject",
			Durable:    "durable",
			BatchSize:  1,
			MaxWait:    time.Millisecond,
			Workers:    1,
			QueueSize:  1,
			AckWait:    time.Second,
			MaxDeliver: 1,
		}

		cfgBad := base
		cfgBad.Durable = ""
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrEmptyDurableName))

		cfgBad = base
		cfgBad.BatchSize = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidBatchSize))

		cfgBad = base
		cfgBad.MaxWait = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidMaxWait))

		cfgBad = base
		cfgBad.Workers = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidWorkerCount))

		cfgBad = base
		cfgBad.QueueSize = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidQueueSize))

		cfgBad = base
		cfgBad.AckWait = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidAckWait))

		cfgBad = base
		cfgBad.MaxDeliver = 0
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidMaxDeliver))

		cfgBad = base
		cfgBad.Adaptive = config.PullAdaptiveConfig{Enabled: true}
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidAdaptiveCheckInterval))

		cfgBad = base
		cfgBad.Adaptive = config.PullAdaptiveConfig{
			Enabled:       true,
			CheckInterval: time.Millisecond,
			MediumPending: 2,
			HighPending:   2,
		}
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidAdaptiveThresholds))

		cfgBad = base
		cfgBad.Adaptive = config.PullAdaptiveConfig{
			Enabled:         true,
			CheckInterval:   time.Millisecond,
			MediumPending:   1,
			HighPending:     2,
			LowBatchSize:    0,
			LowMaxWait:      time.Millisecond,
			MediumBatchSize: 1,
			MediumMaxWait:   time.Millisecond,
			HighBatchSize:   1,
			HighMaxWait:     time.Millisecond,
		}
		Expect(concrete.RunPullConsumer(ctx, cfgBad, func(context.Context, string, []byte) error { return nil })).To(MatchError(ErrInvalidAdaptivePlan))
	})

	It("wraps publish and subscribe failures on a closed connection", func() {
		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		concrete.Close()

		err = concrete.Publish(ctx, cfg.UserCreatedSubject, []byte("payload"))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("publish to nats"))

		err = concrete.Subscribe(ctx, cfg.UserCreatedSubject, func(context.Context, string, []byte) error { return nil })
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("subscribe to nats"))
	})

	It("naks and redelivers when the pull-consumer handler fails", func() {
		rawConn, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer rawConn.Close()

		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())
		concrete := brokerAny.(*natsBroker)
		defer concrete.Close()

		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()

		var handled atomic.Int32
		Expect(concrete.RunPullConsumer(runCtx, config.PullConsumerConfig{
			Stream:     cfg.SagaCommandsStream,
			Subject:    cfg.SagaDeleteUserSubject,
			Durable:    cfg.SagaDeleteUserDurable,
			BatchSize:  1,
			MaxWait:    20 * time.Millisecond,
			Workers:    1,
			QueueSize:  2,
			AckWait:    time.Second,
			MaxDeliver: 3,
		}, func(_ context.Context, _ string, _ []byte) error {
			if handled.Add(1) == 1 {
				return errors.New("retry")
			}
			return nil
		})).To(Succeed())

		Expect(rawConn.Publish(cfg.SagaDeleteUserSubject, []byte("retry-me"))).To(Succeed())
		Expect(rawConn.Flush()).To(Succeed())

		Eventually(func() int32 { return handled.Load() }).Should(BeNumerically(">=", 2))
	})

	It("closes safely", func() {
		brokerAny, err := NewBroker(cfg, brokerSuiteLogger)
		Expect(err).NotTo(HaveOccurred())

		brokerAny.Close()
		brokerAny.Close()
	})

	It("resolves adaptive pull tiers", func() {
		tier, batch, waitFor := ResolvePullPlan(config.PullConsumerConfig{
			BatchSize: 1,
			MaxWait:   50 * time.Millisecond,
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
		}, 25)
		Expect(tier).To(Equal("high"))
		Expect(batch).To(Equal(8))
		Expect(waitFor).To(Equal(10 * time.Millisecond))

		tier, batch, waitFor = ResolvePullPlan(config.PullConsumerConfig{
			BatchSize: 1,
			MaxWait:   50 * time.Millisecond,
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
		}, 12)
		Expect(tier).To(Equal("medium"))
		Expect(batch).To(Equal(4))
		Expect(waitFor).To(Equal(20 * time.Millisecond))

		tier, batch, waitFor = ResolvePullPlan(config.PullConsumerConfig{
			BatchSize: 1,
			MaxWait:   50 * time.Millisecond,
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
		}, 1)
		Expect(tier).To(Equal("low"))
		Expect(batch).To(Equal(2))
		Expect(waitFor).To(Equal(40 * time.Millisecond))

		tier, batch, waitFor = ResolvePullPlan(config.PullConsumerConfig{
			BatchSize: 5,
			MaxWait:   time.Second,
		}, 0)
		Expect(tier).To(Equal("base"))
		Expect(batch).To(Equal(5))
		Expect(waitFor).To(Equal(time.Second))
	})
})

var _ = Describe("pullConsumerRuntime helpers", func() {
	var (
		cancel context.CancelFunc
		cfg    config.NATSConfig
	)

	BeforeEach(func() {
		var suiteCtx context.Context
		suiteCtx, cancel = context.WithTimeout(context.Background(), 45*time.Second)
		if brokerSuiteLogger == nil {
			var err error
			brokerSuiteLogger, err = logging.New("user-service", "test", "debug")
			Expect(err).NotTo(HaveOccurred())
		}
		cfg = uniqueBrokerConfig(brokerSuiteBaseCfg)
		if brokerSuiteContainer != nil {
			Expect(ensureBrokerStreams(cfg)).To(Succeed())
		}
		_ = suiteCtx
	})

	AfterEach(func() {
		cancel()
	})

	It("updates the adaptive plan from pending consumer info", func() {
		if brokerSuiteContainer == nil {
			Skip("Docker is not available for the NATS integration suite")
		}

		nc, err := nats.Connect(cfg.URL)
		Expect(err).NotTo(HaveOccurred())
		defer nc.Close()

		factory := newPullConsumerRuntimeFactory()
		runtimeAny, err := factory.Create(nc, brokerSuiteLogger, config.PullConsumerConfig{
			Stream:     cfg.SagaCommandsStream,
			Subject:    cfg.SagaCreateUserSubject,
			Durable:    cfg.SagaCreateUserDurable,
			BatchSize:  1,
			MaxWait:    time.Second,
			Workers:    1,
			QueueSize:  2,
			AckWait:    time.Second,
			MaxDeliver: 3,
			Adaptive: config.PullAdaptiveConfig{
				Enabled:         true,
				CheckInterval:   time.Millisecond,
				MediumPending:   1,
				HighPending:     2,
				LowBatchSize:    1,
				LowMaxWait:      time.Second,
				MediumBatchSize: 2,
				MediumMaxWait:   500 * time.Millisecond,
				HighBatchSize:   3,
				HighMaxWait:     100 * time.Millisecond,
			},
		}, func(context.Context, string, []byte) error { return nil })
		Expect(err).NotTo(HaveOccurred())

		runtime := runtimeAny.(*pullConsumerRuntime)
		Expect(nc.Publish(cfg.SagaCreateUserSubject, []byte("one"))).To(Succeed())
		Expect(nc.Publish(cfg.SagaCreateUserSubject, []byte("two"))).To(Succeed())
		Expect(nc.Flush()).To(Succeed())

		state := pullConsumerFetchState{
			batch:             1,
			wait:              time.Second,
			tier:              "base",
			lastAdaptiveCheck: time.Now().Add(-time.Second),
		}

		Eventually(func() string {
			runtime.maybeUpdateAdaptivePlan(&state)
			return state.tier
		}).Should(Equal("high"))
		Expect(state.batch).To(Equal(3))
		Expect(state.wait).To(Equal(100 * time.Millisecond))
	})

	It("covers the runtime helper branches", func() {
		runtime := &pullConsumerRuntime{
			log:  brokerSuiteLogger,
			cfg:  config.PullConsumerConfig{Subject: "subject", Durable: "durable", Adaptive: config.PullAdaptiveConfig{Enabled: true, CheckInterval: time.Second}},
			jobs: make(chan *nats.Msg, 1),
		}

		Expect(runtime.shouldStop(context.Background())).To(BeFalse())

		stoppedCtx, stop := context.WithCancel(context.Background())
		stop()
		Expect(runtime.shouldStop(stoppedCtx)).To(BeTrue())

		state := pullConsumerFetchState{
			batch:             1,
			wait:              time.Second,
			tier:              "base",
			lastAdaptiveCheck: time.Now(),
		}
		runtime.maybeUpdateAdaptivePlan(&state)
		Expect(state.tier).To(Equal("base"))

		Expect(runtime.handleFetchError(nil)).To(BeFalse())
		Expect(runtime.handleFetchError(nats.ErrTimeout)).To(BeTrue())
		Expect(runtime.handleFetchError(errors.New("boom"))).To(BeTrue())

		msg := &nats.Msg{Subject: "subject", Data: []byte("payload")}
		Expect(runtime.dispatchFetchedMessages(context.Background(), []*nats.Msg{msg})).To(BeFalse())
		Expect(runtime.jobs).To(Receive(Equal(msg)))

		runtime.jobs = make(chan *nats.Msg)
		cancelledCtx, cancelDispatch := context.WithCancel(context.Background())
		cancelDispatch()
		Expect(runtime.dispatchFetchedMessages(cancelledCtx, []*nats.Msg{msg})).To(BeTrue())
	})
})

var _ = Describe("broker wrappers", func() {
	It("preserves wrapped causes", func() {
		cause := errors.New("boom")

		Expect(WrapConnectToNATSError(cause)).To(MatchError(ContainSubstring("connect to nats")))
		Expect(WrapPublishToNATSError("subject", cause)).To(MatchError(ContainSubstring("publish to nats (subject)")))
		Expect(WrapSubscribeToNATSError("subject", cause)).To(MatchError(ContainSubstring("subscribe to nats (subject)")))
		Expect(WrapFlushNATSPublisherError(cause)).To(MatchError(ContainSubstring("flush nats publisher")))
		Expect(WrapInitJetStreamContextError(cause)).To(MatchError(ContainSubstring("init jetstream context")))
		Expect(WrapEnsureConsumerError("STREAM", "durable", cause, cause)).To(MatchError(ContainSubstring(`ensure consumer "durable" in stream "STREAM"`)))
		Expect(WrapCreatePullSubscriberError("subject", "durable", cause)).To(MatchError(ContainSubstring(`create pull subscriber subject="subject" durable="durable"`)))
	})
})

func startBrokerNATSContainer(ctx context.Context) (testcontainers.Container, config.NATSConfig) {
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
		URL:                         "nats://" + host + ":" + port.Port(),
		UserEventsStream:            "USER_EVENTS",
		UserCreatedSubject:          "user.created",
		SagaCommandsStream:          "SAGA_USER_COMMANDS",
		SagaCreateUserSubject:       "saga.user.create",
		SagaDeleteUserSubject:       "saga.user.delete",
		SagaCreateUserResultSubject: "saga.user.create.result",
		SagaDeleteUserResultSubject: "saga.user.delete.result",
		SagaCreateUserDurable:       "user_service_saga_create",
		SagaDeleteUserDurable:       "user_service_saga_delete",
	}
}

func uniqueBrokerConfig(base config.NATSConfig) config.NATSConfig {
	suffix := time.Now().UTC().Format("150405000000000")
	base.UserEventsStream += "_" + suffix
	base.UserCreatedSubject += "." + suffix
	base.SagaCommandsStream += "_" + suffix
	base.SagaCreateUserSubject += "." + suffix
	base.SagaDeleteUserSubject += "." + suffix
	base.SagaCreateUserResultSubject += "." + suffix
	base.SagaDeleteUserResultSubject += "." + suffix
	base.SagaCreateUserDurable += "_" + suffix
	base.SagaDeleteUserDurable += "_" + suffix
	return base
}

func ensureBrokerStreams(cfg config.NATSConfig) error {
	nc, err := nats.Connect(cfg.URL)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return err
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:      cfg.SagaCommandsStream,
		Subjects:  []string{cfg.SagaCreateUserSubject, cfg.SagaDeleteUserSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
	})
	return err
}
