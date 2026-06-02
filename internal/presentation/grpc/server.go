package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
	"user-service/config"
	app "user-service/internal/application"
	domain "user-service/internal/domain"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	userv1 "github.com/ofm-microservices/ofm-common/proto/user/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	userv1.UnimplementedUserQueryServiceServer
	svc      app.UserService
	cfg      config.GRPCConfig
	log      logging.Logger
	mapr     *userMapper
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

	grpcSrv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(metrics.UnaryServerInterceptor()),
	)
	s := &server{
		svc:  svc,
		cfg:  cfg,
		log:  log.With(logging.String("module", "grpc-server")),
		mapr: newUserMapper(),
		srv:  grpcSrv,
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
	started := time.Now()
	log := logging.WithContext(ctx, s.log)
	exists, err := s.svc.ExistsByUsername(ctx, req.GetUsername())
	if err != nil {
		log.Error("exists by username failed",
			logging.Operation("grpc.user.exists_by_username"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("username", req.GetUsername()),
			logging.Err(err),
		)
		return nil, status.Error(codes.Internal, "internal server error")
	}

	return &userv1.ExistsByUsernameResponse{Exists: exists}, nil
}

// ActivateUser marks a saga-created user profile as active.
func (s *server) ActivateUser(ctx context.Context, req *userv1.ActivateUserRequest) (*userv1.ActivateUserResponse, error) {
	started := time.Now()
	log := logging.WithContext(ctx, s.log)
	user, err := s.svc.ActivateUser(ctx, req.GetUserId())
	if err != nil {
		log.Error("activate user failed",
			logging.Operation("grpc.user.activate"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", req.GetUserId()),
			logging.Err(err),
		)
		return nil, status.Error(codes.Internal, "internal server error")
	}

	return &userv1.ActivateUserResponse{UserId: user.ID, Status: user.Status}, nil
}

// DeactivateUser marks a saga-created user profile inactive as compensation.
func (s *server) DeactivateUser(ctx context.Context, req *userv1.DeactivateUserRequest) (*userv1.DeactivateUserResponse, error) {
	started := time.Now()
	log := logging.WithContext(ctx, s.log)
	if err := s.svc.DeactivateUser(ctx, req.GetUserId()); err != nil {
		log.Error("deactivate user failed",
			logging.Operation("grpc.user.deactivate"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", req.GetUserId()),
			logging.Err(err),
		)
		return nil, status.Error(codes.Internal, "internal server error")
	}

	return &userv1.DeactivateUserResponse{UserId: req.GetUserId(), Status: "registration_failed"}, nil
}

// GetUserPreviewByID returns the cached or lazily loaded user preview.
func (s *server) GetUserPreviewByID(ctx context.Context, req *userv1.GetUserPreviewByIDRequest) (*userv1.GetUserPreviewByIDResponse, error) {
	started := time.Now()
	log := logging.WithContext(ctx, s.log)
	user, err := s.svc.GetUserPreviewByID(ctx, req.GetUserId())
	if err != nil {
		log.Error("get user preview failed",
			logging.Operation("grpc.user.preview"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", req.GetUserId()),
			logging.Err(err),
		)
		return nil, userQueryStatus(err)
	}

	return s.mapr.ToPreviewResponse(user), nil
}

// GetUserPreviewByIDNoCache returns a user preview without touching the Redis read model.
func (s *server) GetUserPreviewByIDNoCache(ctx context.Context, req *userv1.GetUserPreviewByIDNoCacheRequest) (*userv1.GetUserPreviewByIDNoCacheResponse, error) {
	started := time.Now()
	log := logging.WithContext(ctx, s.log)
	user, err := s.svc.GetUserPreviewByIDNoCache(ctx, req.GetUserId())
	if err != nil {
		log.Error("get user preview no cache failed",
			logging.Operation("grpc.user.preview_no_cache"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("user_id", req.GetUserId()),
			logging.Err(err),
		)
		return nil, userQueryStatus(err)
	}

	return &userv1.GetUserPreviewByIDNoCacheResponse{
		User: s.mapr.ToPreviewResponse(user).GetUser(),
	}, nil
}

// GetDetailedUserByUsername returns the cached or lazily loaded detailed user.
func (s *server) GetDetailedUserByUsername(ctx context.Context, req *userv1.GetDetailedUserByUsernameRequest) (*userv1.GetDetailedUserByUsernameResponse, error) {
	started := time.Now()
	log := logging.WithContext(ctx, s.log)
	user, err := s.svc.GetDetailedUserByUsername(ctx, req.GetUsername())
	if err != nil {
		log.Error("get detailed user failed",
			logging.Operation("grpc.user.detailed"),
			logging.Attempt(1),
			logging.Retryable(false),
			logging.DurationMS(time.Since(started)),
			logging.String("username", req.GetUsername()),
			logging.Err(err),
		)
		return nil, userQueryStatus(err)
	}

	return s.mapr.ToDetailedResponse(user), nil
}

func userQueryStatus(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrUserNotFound) {
		return status.Error(codes.NotFound, "user not found")
	}
	return status.Error(codes.Internal, "internal server error")
}
