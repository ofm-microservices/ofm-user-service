package appfx

import "go.uber.org/fx"

// Module is the compatibility bundle of all FX modules used by user-service.
var Module = fx.Options(
	ConfigModule,
	LoggerModule,
	TracingModule,
	MetricsModule,
	AppModule,
	StorageModule,
	OutboundModule,
	MessagingModule,
	RepoModule,
	ServiceModule,
	PresentationModule,
)
