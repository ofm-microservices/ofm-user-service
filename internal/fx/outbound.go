package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"user-service/config"
	app "user-service/internal/application"
	filegrpc "user-service/internal/infra/file/grpc"
	eventbroker "user-service/internal/presentation/event_broker"
	kafkapub "user-service/internal/presentation/event_broker/kafka"

	"go.uber.org/fx"
)

// OutboundModule wires outbound gRPC clients and projection publishers used by
// user-service.
var OutboundModule = fx.Options(
	fx.Provide(
		ProvideFileURLClient,
		ProvideDetailedUserPublisher,
	),
)

// ProvideFileURLClient constructs the file-service client used for avatar URL
// enrichment.
func ProvideFileURLClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.FileURLClient, error) {
	client, err := filegrpc.New(cfg.File, lg)
	if err != nil {
		lg.Error("open file service grpc client failed", logging.Err(err))
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return client.Close()
		},
	})

	return client, nil
}

// ProvideDetailedUserPublisher constructs the detailed user projection
// publisher.
func ProvideDetailedUserPublisher(broker eventbroker.EventBroker, cfg *config.Config, lg logging.Logger) (app.DetailedUserPublisher, error) {
	return kafkapub.NewDetailedUserPublisher(broker, cfg.Kafka, lg)
}
