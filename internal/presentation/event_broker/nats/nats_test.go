package nats

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ofm-microseervices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"user-service/config"
	user "user-service/internal/domain"
	eventbroker "user-service/internal/presentation/event_broker"
)

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
