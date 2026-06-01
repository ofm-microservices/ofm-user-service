package main

import (
	appfx "user-service/internal/fx"

	"go.uber.org/fx"
)

func newApp() *fx.App {
	return fx.New(
		appfx.ConfigModule,
		appfx.LoggerModule,
		appfx.TracingModule,
		appfx.MetricsModule,
		appfx.AppModule,
		appfx.StorageModule,
		appfx.OutboundModule,
		appfx.MessagingModule,
		appfx.RepoModule,
		appfx.ServiceModule,
		appfx.PresentationModule,
	)
}

var runApp = (*fx.App).Run

func main() {
	runApp(newApp())
}
