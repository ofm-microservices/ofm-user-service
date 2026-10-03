package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"user-service/config"
	app "user-service/internal/application"
	eventbroker "user-service/internal/presentation/event_broker"
)

// UserRecoverySubscriber consumes user-owned monolith fallback commands.
type UserRecoverySubscriber interface{ Subscribe(context.Context) error }

type userRecoverySubscriber struct {
	broker  eventbroker.EventBroker
	service app.UserService
	cfg     config.KafkaConfig
	log     logging.Logger
}

// NewUserRecoverySubscriber constructs the user-owned recovery adapter.
func NewUserRecoverySubscriber(b eventbroker.EventBroker, service app.UserService, cfg config.KafkaConfig, log logging.Logger) (UserRecoverySubscriber, error) {
	if b == nil || service == nil || log == nil {
		return nil, errors.New("invalid user recovery subscriber dependency")
	}
	return &userRecoverySubscriber{broker: b, service: service, cfg: cfg, log: log.With(logging.String("module", "kafka-user-recovery-subscriber"))}, nil
}

func (s *userRecoverySubscriber) Subscribe(ctx context.Context) error {
	return s.broker.RunPullConsumer(ctx, config.PullConsumerConfig{Subject: s.cfg.RecoveryTopic, GroupID: s.cfg.RecoveryGroup}, s.handle)
}

func (s *userRecoverySubscriber) handle(ctx context.Context, _ string, raw []byte) error {
	var command events.Envelope
	if err := json.Unmarshal(raw, &command); err != nil {
		return fmt.Errorf("decode user recovery command: %w", err)
	}
	if command.AggregateType != "user" && command.AggregateType != "users" {
		return fmt.Errorf("unsupported user recovery aggregate_type=%q", command.AggregateType)
	}
	var result any
	var err error
	userID := command.AggregateID
	switch strings.ToLower(command.Operation) {
	case "post", "create":
		var p struct {
			UserID    string `json:"user_id"`
			Username  string `json:"username"`
			FirstName string `json:"first_name"`
			LastName  string `json:"last_name"`
		}
		if err = json.Unmarshal(command.Payload, &p); err != nil {
			return err
		}
		if p.UserID == "" {
			p.UserID = userID
		}
		if p.UserID == "" {
			p.UserID = command.RecoveryPrincipalID
		}
		if strings.TrimSpace(p.UserID) == "" {
			return resilience.Permanent(fmt.Errorf("user recovery command %s is missing user_id", command.CommandID))
		}
		userID = p.UserID
		var user any
		user, err = s.service.CreateUser(ctx, p.UserID, p.Username, p.FirstName, p.LastName)
		result = user
	case "delete":
		err = s.service.DeleteUser(ctx, userID)
	case "activate":
		var user any
		user, err = s.service.ActivateUser(ctx, userID)
		result = user
	case "deactivate":
		err = s.service.DeactivateUser(ctx, userID)
	default:
		return fmt.Errorf("unsupported user recovery operation=%s", command.Operation)
	}
	if err != nil {
		return err
	}
	completed := events.Envelope{EventID: command.EventID + ".completed", CommandID: command.CommandID, CorrelationID: command.CorrelationID, CausationID: command.EventID, IdempotencyKey: command.IdempotencyKey, TestRunID: command.TestRunID, EventType: "migration.recovery.completed", Operation: command.Operation, SchemaVersion: 1, AggregateType: "user", AggregateID: userID, SourceService: "user-service-recovery", OccurredAt: time.Now().UTC(), Payload: mustJSON(result)}
	body, err := json.Marshal(completed)
	if err != nil {
		return err
	}
	s.log.Info("user recovery command completed", logging.Operation("user.recovery.completed"), logging.String("command_id", command.CommandID), logging.String("aggregate_id", userID))
	return s.broker.Publish(context.WithoutCancel(ctx), s.cfg.RecoveryCompletedTopic, body)
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
