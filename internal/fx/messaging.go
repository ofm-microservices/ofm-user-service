package appfx

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"user-service/config"
	eventbroker "user-service/internal/presentation/event_broker"
	broker "user-service/internal/presentation/event_broker/kafka"

	"go.uber.org/fx"
)

// MessagingModule wires the Kafka broker runtime into user-service.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBrokerWithDB),
)

var newEventBroker = broker.NewBroker
var ensureStream = func(_ config.NATSConfig, _ logging.Logger) error { return nil }

// InvokeEnsureStream ensures the JetStream streams user-service depends on.
// InvokeEnsureStream is retained as a compatibility no-op; Kafka topics are
// provisioned by the broker/runtime rather than JetStream bootstrap.
func InvokeEnsureStream(_ *config.Config, lg logging.Logger) error {
	return ensureStream(config.NATSConfig{}, lg)
}

// ProvideEventBroker constructs the concrete Kafka event broker.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (eventbroker.EventBroker, error) {
	return provideEventBroker(lc, cfg, nil, lg)
}

// ProvideEventBrokerWithDB enables durable event claims in production wiring.
func ProvideEventBrokerWithDB(lc fx.Lifecycle, cfg *config.Config, db *sqlx.DB, lg logging.Logger) (eventbroker.EventBroker, error) {
	return provideEventBroker(lc, cfg, db, lg)
}

func provideEventBroker(lc fx.Lifecycle, cfg *config.Config, db *sqlx.DB, lg logging.Logger) (eventbroker.EventBroker, error) {
	var eventBroker eventbroker.EventBroker
	var err error
	if db == nil {
		eventBroker, err = newEventBroker(cfg.Kafka)
	} else {
		eventBroker, err = broker.NewBrokerWithDB(cfg.Kafka, db)
	}
	if err != nil {
		lg.Error("connect kafka failed", logging.Err(err))
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			eventBroker.Close()
			return nil
		},
	})

	return eventBroker, nil
}
