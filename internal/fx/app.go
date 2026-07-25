package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"

	"go.uber.org/fx"
)

// AppModule wires process-level startup hooks for user-service.
var AppModule = fx.Options(
	fx.Invoke(InvokeStartLog),
)

// InvokeStartLog emits the process start log entry once FX wiring completes.
func InvokeStartLog(lg logging.Logger) {
	lg.Info("starting user-service")
}
