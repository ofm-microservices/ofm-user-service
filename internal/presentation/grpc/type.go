package grpc

import "context"

// Server defines the gRPC server lifecycle exposed by user-service.
type Server interface {
	Start() error
	Shutdown(ctx context.Context) error
}
