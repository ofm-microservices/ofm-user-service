package grpc

import (
	"context"
	"fmt"
	"net"
	"user-service/config"
	app "user-service/internal/application"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	userv1 "github.com/ofm-microservices/ofm-common/proto/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	userv1.UnimplementedUserQueryServiceServer
	svc      app.UserService
	cfg      config.GRPCConfig
	log      logging.Logger
	srv      *grpc.Server
	listener net.Listener
}

// NewServer constructs the user-service gRPC query server.
func NewServer(svc app.UserService, cfg config.GRPCConfig, log logging.Logger) (Server, error) {
	if svc == nil {
		return nil, ErrNilUserService
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(metrics.UnaryServerInterceptor()))
	s := &server{
		svc: svc,
		cfg: cfg,
		log: log.With(logging.String("module", "grpc-server")),
		srv: grpcSrv,
	}
	userv1.RegisterUserQueryServiceServer(grpcSrv, s)
	return s, nil
}

// Start begins serving gRPC traffic on the configured address.
func (s *server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.listener = lis
	s.log.Info("starting grpc server", logging.String("addr", addr))
	return s.srv.Serve(lis)
}

// Shutdown gracefully stops the gRPC server.
func (s *server) Shutdown(context.Context) error {
	if s.srv != nil {
		s.log.Info("shutting down grpc server")
		s.srv.GracefulStop()
	}
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// ExistsByUsername answers whether user-service already owns the supplied
// username.
func (s *server) ExistsByUsername(ctx context.Context, req *userv1.ExistsByUsernameRequest) (*userv1.ExistsByUsernameResponse, error) {
	exists, err := s.svc.ExistsByUsername(ctx, req.GetUsername())
	if err != nil {
		s.log.Error("exists by username failed", logging.Err(err))
		return nil, status.Error(codes.Internal, "internal server error")
	}

	return &userv1.ExistsByUsernameResponse{Exists: exists}, nil
}

// ActivateUser marks a saga-created user profile as active.
func (s *server) ActivateUser(ctx context.Context, req *userv1.ActivateUserRequest) (*userv1.ActivateUserResponse, error) {
	user, err := s.svc.ActivateUser(ctx, req.GetUserId())
	if err != nil {
		s.log.Error("activate user failed", logging.String("user_id", req.GetUserId()), logging.Err(err))
		return nil, status.Error(codes.Internal, "internal server error")
	}

	return &userv1.ActivateUserResponse{UserId: user.ID, Status: user.Status}, nil
}

// DeactivateUser marks a saga-created user profile inactive as compensation.
func (s *server) DeactivateUser(ctx context.Context, req *userv1.DeactivateUserRequest) (*userv1.DeactivateUserResponse, error) {
	if err := s.svc.DeactivateUser(ctx, req.GetUserId()); err != nil {
		s.log.Error("deactivate user failed", logging.String("user_id", req.GetUserId()), logging.Err(err))
		return nil, status.Error(codes.Internal, "internal server error")
	}

	return &userv1.DeactivateUserResponse{UserId: req.GetUserId(), Status: "registration_failed"}, nil
}
