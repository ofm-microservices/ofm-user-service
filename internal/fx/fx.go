package appfx

import "go.uber.org/fx"

// Module is the compatibility bundle of all FX modules used by user-service.
var Module = fx.Options(
	ConfigModule,
	LoggerModule,
	MetricsModule,
	AppModule,
	StorageModule,
	MessagingModule,
	RepoModule,
	ServiceModule,
	PresentationModule,
)
