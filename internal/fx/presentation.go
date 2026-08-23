package appfx

import (
	"context"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"user-service/config"
	app "user-service/internal/application"
	user "user-service/internal/domain"
	eventbroker "user-service/internal/presentation/event_broker"
	kafkaevents "user-service/internal/presentation/event_broker/kafka"
	grpcserver "user-service/internal/presentation/grpc"

	"go.uber.org/fx"
)

// PresentationModule wires transport adapters and background subscribers into
// the FX lifecycle.
var PresentationModule = fx.Options(
	fx.Provide(
		kafkaevents.NewDomainFailureReasonResolver,
		ProvideRegistrationSagaSubscriber,
		ProvideDetailedUserProjectionRelay,
		ProvideDetailedUserProjectionSubscriber,
		ProvideGRPCServer,
	),
	fx.Invoke(
		InvokeSubscribeRegistrationSaga,
		InvokeStartDetailedUserProjectionRelay,
		InvokeStartDetailedUserProjectionSubscriber,
		InvokeRunGRPCServer,
	),
)

// ProvideRegistrationSagaSubscriber constructs the Kafka subscriber that
// consumes registration-saga commands.
func ProvideRegistrationSagaSubscriber(
	broker eventbroker.EventBroker,
	service app.UserService,
	cfg *config.Config,
	resolver kafkaevents.FailureReasonResolver,
	lg logging.Logger,
) (kafkaevents.RegistrationSagaSubscriber, error) {
	return kafkaevents.NewRegistrationSagaSubscriber(broker, service, cfg.Kafka, resolver, lg)
}

// ProvideDetailedUserProjectionRelay constructs the relay that forwards
// detailed-user request events into the durable projection subject.
func ProvideDetailedUserProjectionRelay(
	broker eventbroker.EventBroker,
	cfg *config.Config,
	lg logging.Logger,
) (kafkaevents.DetailedUserProjectionRelay, error) {
	return kafkaevents.NewDetailedUserProjectionRelay(broker, cfg.Kafka, lg)
}

// ProvideDetailedUserProjectionSubscriber constructs the Redis projection
// worker for detailed user data.
func ProvideDetailedUserProjectionSubscriber(
	broker eventbroker.EventBroker,
	read user.UserReadRepository,
	cfg *config.Config,
	lg logging.Logger,
) (kafkaevents.DetailedUserProjectionSubscriber, error) {
	return kafkaevents.NewDetailedUserProjectionSubscriber(broker, read, cfg.Kafka, lg)
}

// ProvideGRPCServer constructs the gRPC query server exposed by user-service.
func ProvideGRPCServer(
	service app.UserService,
	cfg *config.Config,
	lg logging.Logger,
) (grpcserver.Server, error) {
	return grpcserver.NewServer(service, cfg.GRPC, lg)
}

// InvokeSubscribeRegistrationSaga starts background consumers for registration
// saga commands.
func InvokeSubscribeRegistrationSaga(
	lc fx.Lifecycle,
	subscriber kafkaevents.RegistrationSagaSubscriber,
	cfg *config.Config,
	lg logging.Logger,
) {
	var cancel context.CancelFunc

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel

			go func() {
				if err := subscriber.Subscribe(runCtx); err != nil && runCtx.Err() == nil {
					lg.Error("subscribe to registration saga commands failed", logging.Err(err))
				}
			}()

			lg.Info("user-service initialized", logging.String("env", cfg.App.Env))
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
}

// InvokeRunGRPCServer starts and gracefully stops the gRPC server with the FX
// lifecycle.
func InvokeRunGRPCServer(lc fx.Lifecycle, srv grpcserver.Server) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				if err := srv.Start(); err != nil {
					panic(err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		},
	})
}

// InvokeStartDetailedUserProjectionRelay starts the detailed-user request
// relay.
func InvokeStartDetailedUserProjectionRelay(lc fx.Lifecycle, relay kafkaevents.DetailedUserProjectionRelay) {
	var cancel context.CancelFunc

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel

			go func() {
				if err := relay.Start(runCtx); err != nil {
					panic(err)
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
}

// InvokeStartDetailedUserProjectionSubscriber starts the detailed-user Redis
// projection worker.
func InvokeStartDetailedUserProjectionSubscriber(lc fx.Lifecycle, subscriber kafkaevents.DetailedUserProjectionSubscriber) {
	var cancel context.CancelFunc

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel

			go func() {
				if err := subscriber.Start(runCtx); err != nil {
					panic(err)
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
			}
			return nil
		},
	})
}
