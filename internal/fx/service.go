package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	app "user-service/internal/application"
	user "user-service/internal/domain"

	"go.uber.org/fx"
)

// ServiceModule provides the application service used by user-service.
var ServiceModule = fx.Options(
	fx.Provide(ProvideUserService),
)

// ProvideUserService constructs the user-service application service.
func ProvideUserService(writeRepo user.UserRepository, readRepo user.UserReadRepository, files app.FileURLClient, pub app.DetailedUserPublisher, lg logging.Logger) (app.UserService, error) {
	return app.NewDetailed(writeRepo, readRepo, files, pub, lg)
}
