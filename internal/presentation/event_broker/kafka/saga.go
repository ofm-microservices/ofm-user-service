package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"user-service/config"
	app "user-service/internal/application"
	eventbroker "user-service/internal/presentation/event_broker"
)

// FailureReasonResolver converts domain failures into stable saga reasons.
type FailureReasonResolver interface {
	CreateUserFailureReason(error) string
	DeleteUserFailureReason(error) string
}

// RegistrationSagaSubscriber consumes registration commands and publishes
// user-operation results over Kafka.
type RegistrationSagaSubscriber interface{ Subscribe(context.Context) error }

type createUserCommand struct {
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	SagaID    string `json:"saga_id,omitempty"`
}
type deleteUserCommand struct {
	SessionID string `json:"session_id,omitempty"`
	UserID    string `json:"user_id"`
	SagaID    string `json:"saga_id,omitempty"`
}
type sagaResult struct {
	SessionID string `json:"session_id,omitempty"`
	UserID    string `json:"user_id"`
	SagaID    string `json:"saga_id,omitempty"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	Timestamp string `json:"timestamp"`
}

type registrationSagaSubscriber struct {
	broker   eventbroker.EventBroker
	service  app.UserService
	resolver FailureReasonResolver
	cfg      config.KafkaConfig
	log      logging.Logger
}

// NewRegistrationSagaSubscriber constructs the Kafka registration-saga
// command subscriber.
func NewRegistrationSagaSubscriber(b eventbroker.EventBroker, service app.UserService, cfg config.KafkaConfig, resolver FailureReasonResolver, log logging.Logger) (RegistrationSagaSubscriber, error) {
	if b == nil || service == nil || resolver == nil || log == nil {
		return nil, errors.New("invalid Kafka registration subscriber dependency")
	}
	if strings.TrimSpace(cfg.SagaCreateUserTopic) == "" || strings.TrimSpace(cfg.SagaDeleteUserTopic) == "" || strings.TrimSpace(cfg.SagaCreateResultTopic) == "" || strings.TrimSpace(cfg.SagaDeleteResultTopic) == "" {
		return nil, errors.New("Kafka registration topic is empty")
	}
	return &registrationSagaSubscriber{broker: b, service: service, resolver: resolver, cfg: cfg, log: log.With(logging.String("module", "kafka-registration-saga-subscriber"))}, nil
}

func (s *registrationSagaSubscriber) Subscribe(ctx context.Context) error {
	if err := s.broker.Subscribe(ctx, s.cfg.SagaCreateUserTopic, s.handleCreate); err != nil {
		return err
	}
	return s.broker.Subscribe(ctx, s.cfg.SagaDeleteUserTopic, s.handleDelete)
}

func (s *registrationSagaSubscriber) handleCreate(ctx context.Context, _ string, raw []byte) error {
	s.log.Info("registration command handler entered", logging.Int("payload_bytes", len(raw)))
	var cmd createUserCommand
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return err
	}
	if _, err := s.service.CreateUser(ctx, cmd.UserID, cmd.Username, cmd.FirstName, cmd.LastName); err != nil {
		s.log.Error("registration command application failed", logging.String("user_id", cmd.UserID), logging.Err(err))
		return s.publish(ctx, s.cfg.SagaCreateResultTopic, sagaResult{SessionID: cmd.SessionID, UserID: cmd.UserID, SagaID: cmd.SagaID, Status: "failed", Error: s.resolver.CreateUserFailureReason(err)})
	}
	err := s.publish(ctx, s.cfg.SagaCreateResultTopic, sagaResult{SessionID: cmd.SessionID, UserID: cmd.UserID, SagaID: cmd.SagaID, Status: "success"})
	if err != nil {
		s.log.Error("registration result publish failed", logging.String("topic", s.cfg.SagaCreateResultTopic), logging.String("session_id", cmd.SessionID), logging.Err(err))
	} else {
		s.log.Info("registration result published", logging.String("topic", s.cfg.SagaCreateResultTopic), logging.String("session_id", cmd.SessionID))
	}
	return err
}

func (s *registrationSagaSubscriber) handleDelete(ctx context.Context, _ string, raw []byte) error {
	var cmd deleteUserCommand
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return err
	}
	if err := s.service.DeleteUser(ctx, cmd.UserID); err != nil {
		return s.publish(ctx, s.cfg.SagaDeleteResultTopic, sagaResult{SessionID: cmd.SessionID, UserID: cmd.UserID, SagaID: cmd.SagaID, Status: "failed", Error: s.resolver.DeleteUserFailureReason(err)})
	}
	return s.publish(ctx, s.cfg.SagaDeleteResultTopic, sagaResult{SessionID: cmd.SessionID, UserID: cmd.UserID, SagaID: cmd.SagaID, Status: "success"})
}

func (s *registrationSagaSubscriber) publish(ctx context.Context, topic string, result sagaResult) error {
	result.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return s.broker.Publish(ctx, topic, payload)
}
