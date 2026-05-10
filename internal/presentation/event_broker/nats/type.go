package nats

import (
	"context"
	"user-service/config"
	eventbroker "user-service/internal/presentation/event_broker"

	"github.com/nats-io/nats.go"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

// RegistrationSagaSubscriber consumes user-related saga commands from NATS.
type RegistrationSagaSubscriber interface {
	Subscribe(ctx context.Context) error
}

// RegistrationSagaMessageMapper translates user registration results into NATS
// payloads consumed by the registration saga.
type RegistrationSagaMessageMapper interface {
	ToCreateFailureResultPayload(cmd createUserCommand, reason string) ([]byte, error)
	ToCreateSuccessResultPayload(cmd createUserCommand) ([]byte, error)
	ToDeleteFailureResultPayload(cmd deleteUserCommand, reason string) ([]byte, error)
	ToDeleteSuccessResultPayload(cmd deleteUserCommand) ([]byte, error)
}

// PullConsumerConfigValidator validates pull-consumer runtime configuration
// before the broker touches JetStream state.
type PullConsumerConfigValidator interface {
	Validate(cfg config.PullConsumerConfig) error
}

// PullConsumerRuntime represents one configured JetStream pull-consumer
// runtime.
type PullConsumerRuntime interface {
	Start(ctx context.Context)
}

// PullConsumerRuntimeFactory builds the runtime used by the broker after the
// config is validated.
type PullConsumerRuntimeFactory interface {
	Create(
		nc *nats.Conn,
		log logging.Logger,
		cfg config.PullConsumerConfig,
		handler eventbroker.MessageHandler,
	) (PullConsumerRuntime, error)
}
