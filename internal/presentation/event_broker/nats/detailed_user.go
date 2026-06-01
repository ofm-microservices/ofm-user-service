package nats

import (
	"context"
	"encoding/json"
	"strings"

	"user-service/config"
	app "user-service/internal/application"
	domain "user-service/internal/domain"
	eventbroker "user-service/internal/presentation/event_broker"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

type detailedUserRequestedEvent struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	AvatarID    string `json:"avatar_id"`
	AvatarURL   string `json:"avatar_url"`
	About       string `json:"about"`
}

type detailedUserProjectionEvent = detailedUserRequestedEvent

// DetailedUserPublisher emits best-effort detailed-user request events that
// warm the public user cache asynchronously.
type DetailedUserPublisher struct {
	broker eventbroker.EventBroker
	cfg    config.NATSConfig
	log    logging.Logger
}

// NewDetailedUserPublisher constructs the detailed user projection publisher.
func NewDetailedUserPublisher(broker eventbroker.EventBroker, cfg config.NATSConfig, log logging.Logger) (app.DetailedUserPublisher, error) {
	if broker == nil {
		return nil, ErrNilBroker
	}
	if strings.TrimSpace(cfg.UserDetailedRequestedSubject) == "" {
		return nil, ErrEmptySubject
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &DetailedUserPublisher{
		broker: broker,
		cfg:    cfg,
		log:    log.With(logging.String("module", "detailed-user-publisher")),
	}, nil
}

// PublishDetailedUserRequested emits the detailed-user cache warm request.
func (p *DetailedUserPublisher) PublishDetailedUserRequested(ctx context.Context, user *domain.User) error {
	if user == nil {
		return ErrInvalidDetailedUserPayload
	}

	payload, err := json.Marshal(detailedUserRequestedEvent{
		UserID:      strings.TrimSpace(user.ID),
		Username:    strings.TrimSpace(user.Username),
		DisplayName: strings.TrimSpace(user.DisplayName),
		AvatarID:    strings.TrimSpace(user.AvatarID),
		AvatarURL:   strings.TrimSpace(user.AvatarURL),
		About:       strings.TrimSpace(user.About),
	})
	if err != nil {
		return WrapMarshalDetailedUserRequestedError(err)
	}

	return p.broker.Publish(ctx, p.cfg.UserDetailedRequestedSubject, payload)
}

// DetailedUserProjectionRelay forwards detailed-user requests into the durable
// projection subject.
type detailedUserProjectionRelay struct {
	broker eventbroker.EventBroker
	cfg    config.NATSConfig
	log    logging.Logger
}

// NewDetailedUserProjectionRelay constructs the relay that republishes the
// best-effort detailed user request into the durable projection stream.
func NewDetailedUserProjectionRelay(broker eventbroker.EventBroker, cfg config.NATSConfig, log logging.Logger) (DetailedUserProjectionRelay, error) {
	if broker == nil {
		return nil, ErrNilBroker
	}
	if strings.TrimSpace(cfg.UserDetailedRequestedSubject) == "" {
		return nil, ErrEmptySubject
	}
	if strings.TrimSpace(cfg.UserDetailedProjectionSubject) == "" {
		return nil, ErrEmptySubject
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &detailedUserProjectionRelay{
		broker: broker,
		cfg:    cfg,
		log:    log.With(logging.String("module", "detailed-user-relay")),
	}, nil
}

// Start wires the request subject to the projection subject.
func (r *detailedUserProjectionRelay) Start(ctx context.Context) error {
	if strings.TrimSpace(r.cfg.UserDetailedRequestedSubject) == "" {
		return ErrEmptySubject
	}
	if strings.TrimSpace(r.cfg.UserDetailedProjectionSubject) == "" {
		return ErrEmptySubject
	}

	return r.broker.Subscribe(ctx, r.cfg.UserDetailedRequestedSubject, func(msgCtx context.Context, subject string, payload []byte) error {
		if len(payload) == 0 {
			return ErrInvalidDetailedUserPayload
		}
		return r.broker.Publish(msgCtx, r.cfg.UserDetailedProjectionSubject, payload)
	})
}

// DetailedUserProjectionSubscriber writes the detailed user projection to
// Redis.
type detailedUserProjectionSubscriber struct {
	broker eventbroker.EventBroker
	read   domain.UserReadRepository
	cfg    config.NATSConfig
	log    logging.Logger
}

// NewDetailedUserProjectionSubscriber constructs the Redis projection worker
// for detailed users.
func NewDetailedUserProjectionSubscriber(broker eventbroker.EventBroker, read domain.UserReadRepository, cfg config.NATSConfig, log logging.Logger) (DetailedUserProjectionSubscriber, error) {
	if broker == nil {
		return nil, ErrNilBroker
	}
	if read == nil {
		return nil, ErrNilUserReadRepository
	}
	if strings.TrimSpace(cfg.UserDetailedStream) == "" {
		return nil, ErrEmptyStreamName
	}
	if strings.TrimSpace(cfg.UserDetailedProjectionSubject) == "" {
		return nil, ErrEmptySubject
	}
	if strings.TrimSpace(cfg.UserDetailedProjectionDurable) == "" {
		return nil, ErrEmptyDurableName
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	return &detailedUserProjectionSubscriber{
		broker: broker,
		read:   read,
		cfg:    cfg,
		log:    log.With(logging.String("module", "detailed-user-projection-subscriber")),
	}, nil
}

// Start starts the durable detailed-user projection consumer.
func (s *detailedUserProjectionSubscriber) Start(ctx context.Context) error {
	cfg := config.PullConsumerConfig{
		Stream:     s.cfg.UserDetailedStream,
		Subject:    s.cfg.UserDetailedProjectionSubject,
		Durable:    s.cfg.UserDetailedProjectionDurable,
		BatchSize:  s.cfg.UserDetailedProjectionBatchSize,
		MaxWait:    s.cfg.UserDetailedProjectionMaxWait,
		Workers:    s.cfg.UserDetailedProjectionWorkers,
		QueueSize:  s.cfg.UserDetailedProjectionQueueSize,
		AckWait:    s.cfg.UserDetailedProjectionAckWait,
		MaxDeliver: s.cfg.UserDetailedProjectionMaxDeliver,
		Adaptive:   config.PullAdaptiveConfig{},
	}

	return s.broker.RunPullConsumer(ctx, cfg, func(msgCtx context.Context, subject string, payload []byte) error {
		var event detailedUserProjectionEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return WrapUnmarshalDetailedUserProjectionError(err)
		}

		user := &domain.User{
			ID:          strings.TrimSpace(event.UserID),
			Username:    strings.TrimSpace(event.Username),
			DisplayName: strings.TrimSpace(event.DisplayName),
			AvatarID:    strings.TrimSpace(event.AvatarID),
			AvatarURL:   strings.TrimSpace(event.AvatarURL),
			About:       strings.TrimSpace(event.About),
		}
		if strings.TrimSpace(user.Username) == "" {
			return ErrInvalidDetailedUserPayload
		}

		return s.read.UpsertByUsername(msgCtx, user)
	})
}
