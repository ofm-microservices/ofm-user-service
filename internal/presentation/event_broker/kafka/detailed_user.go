package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"user-service/config"
	app "user-service/internal/application"
	domain "user-service/internal/domain"
	eventbroker "user-service/internal/presentation/event_broker"
)

var (
	errNilBroker      = errors.New("event broker is nil")
	errEmptyTopic     = errors.New("Kafka detailed-user topic is empty")
	errNilLogger      = errors.New("logger is nil")
	errInvalidPayload = errors.New("invalid detailed user payload")
)

type detailedUserRequestedEvent struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	AvatarID    string `json:"avatar_id"`
	AvatarURL   string `json:"avatar_url"`
	About       string `json:"about"`
}

// DetailedUserPublisher emits Kafka requests that warm the user read model.
type DetailedUserPublisher struct {
	broker eventbroker.EventBroker
	topic  string
	log    logging.Logger
}

// DetailedUserProjectionRelay forwards detailed-user requests to the
// projection topic.
type DetailedUserProjectionRelay interface{ Start(context.Context) error }

// DetailedUserProjectionSubscriber writes detailed-user events to Redis.
type DetailedUserProjectionSubscriber interface{ Start(context.Context) error }

type detailedUserProjectionRelay struct {
	broker        eventbroker.EventBroker
	input, output string
}
type detailedUserProjectionSubscriber struct {
	broker eventbroker.EventBroker
	read   domain.UserReadRepository
	topic  string
}

// NewDetailedUserProjectionRelay constructs the Kafka projection relay.
func NewDetailedUserProjectionRelay(b eventbroker.EventBroker, cfg config.KafkaConfig, log logging.Logger) (DetailedUserProjectionRelay, error) {
	if b == nil {
		return nil, errNilBroker
	}
	if strings.TrimSpace(cfg.DetailedUserRequestedTopic) == "" || strings.TrimSpace(cfg.DetailedUserProjectionTopic) == "" {
		return nil, errEmptyTopic
	}
	if log == nil {
		return nil, errNilLogger
	}
	return &detailedUserProjectionRelay{broker: b, input: cfg.DetailedUserRequestedTopic, output: cfg.DetailedUserProjectionTopic}, nil
}

// Start forwards detailed-user requests through Kafka.
func (r *detailedUserProjectionRelay) Start(ctx context.Context) error {
	return r.broker.Subscribe(ctx, r.input, func(msgCtx context.Context, _ string, payload []byte) error {
		if len(payload) == 0 {
			return errInvalidPayload
		}
		return r.broker.Publish(msgCtx, r.output, payload)
	})
}

// NewDetailedUserProjectionSubscriber constructs the Kafka Redis projection.
func NewDetailedUserProjectionSubscriber(b eventbroker.EventBroker, read domain.UserReadRepository, cfg config.KafkaConfig, log logging.Logger) (DetailedUserProjectionSubscriber, error) {
	if b == nil {
		return nil, errNilBroker
	}
	if read == nil {
		return nil, errors.New("user read repository is nil")
	}
	if strings.TrimSpace(cfg.DetailedUserProjectionTopic) == "" {
		return nil, errEmptyTopic
	}
	if log == nil {
		return nil, errNilLogger
	}
	return &detailedUserProjectionSubscriber{broker: b, read: read, topic: cfg.DetailedUserProjectionTopic}, nil
}

// Start consumes detailed-user events and updates the Redis read model.
func (s *detailedUserProjectionSubscriber) Start(ctx context.Context) error {
	return s.broker.Subscribe(ctx, s.topic, func(msgCtx context.Context, _ string, payload []byte) error {
		var event detailedUserRequestedEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		user := &domain.User{ID: strings.TrimSpace(event.UserID), Username: strings.TrimSpace(event.Username), DisplayName: strings.TrimSpace(event.DisplayName), AvatarID: strings.TrimSpace(event.AvatarID), AvatarURL: strings.TrimSpace(event.AvatarURL), About: strings.TrimSpace(event.About)}
		if user.Username == "" {
			return errInvalidPayload
		}
		return s.read.UpsertByUsername(msgCtx, user)
	})
}

// NewDetailedUserPublisher constructs the Kafka detailed-user publisher.
func NewDetailedUserPublisher(b eventbroker.EventBroker, cfg config.KafkaConfig, log logging.Logger) (app.DetailedUserPublisher, error) {
	if b == nil {
		return nil, errNilBroker
	}
	if strings.TrimSpace(cfg.DetailedUserRequestedTopic) == "" {
		return nil, errEmptyTopic
	}
	if log == nil {
		return nil, errNilLogger
	}
	return &DetailedUserPublisher{broker: b, topic: cfg.DetailedUserRequestedTopic, log: log.With(logging.String("module", "kafka-detailed-user-publisher"))}, nil
}

// PublishDetailedUserRequested publishes a best-effort detailed-user request.
func (p *DetailedUserPublisher) PublishDetailedUserRequested(ctx context.Context, user *domain.User) error {
	if user == nil {
		return errInvalidPayload
	}
	payload, err := json.Marshal(detailedUserRequestedEvent{UserID: strings.TrimSpace(user.ID), Username: strings.TrimSpace(user.Username), DisplayName: strings.TrimSpace(user.DisplayName), AvatarID: strings.TrimSpace(user.AvatarID), AvatarURL: strings.TrimSpace(user.AvatarURL), About: strings.TrimSpace(user.About)})
	if err != nil {
		return err
	}
	return p.broker.Publish(ctx, p.topic, payload)
}
