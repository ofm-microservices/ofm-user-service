package main

import (
	appfx "user-service/internal/fx"

	"go.uber.org/fx"
)

func newApp() *fx.App {
	return fx.New(
		appfx.ConfigModule,
		appfx.LoggerModule,
		appfx.AppModule,
		appfx.StorageModule,
		appfx.MessagingModule,
		appfx.RepoModule,
		appfx.ServiceModule,
		appfx.PresentationModule,
	)
}

var runApp = func(app *fx.App) {
	app.Run()
}

func main() {
	runApp(newApp())
}
