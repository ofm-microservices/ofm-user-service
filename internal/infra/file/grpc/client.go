package grpc

import (
	"context"
	"strings"

	"user-service/config"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	filev1 "github.com/ofm-microservices/ofm-common/proto/file/v1"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type client struct {
	conn *grpcpkg.ClientConn
	cl   filev1.FileServiceClient
	log  logging.Logger
}

// New constructs the user-service file URL client.
func New(cfg config.FileServiceConfig, log logging.Logger) (FileURLClient, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, ErrEmptyAddress
	}
	if log == nil {
		return nil, ErrNilLogger
	}

	conn, err := grpcpkg.NewClient(cfg.Address, grpcpkg.WithTransportCredentials(insecure.NewCredentials()), grpcpkg.WithUnaryInterceptor(metrics.UnaryClientInterceptor()))
	if err != nil {
		return nil, err
	}

	return &client{
		conn: conn,
		cl:   filev1.NewFileServiceClient(conn),
		log:  log.With(logging.String("module", "file-url-client"), logging.String("address", cfg.Address)),
	}, nil
}

func (c *client) GetFileURL(ctx context.Context, fileID string) (string, error) {
	res, err := c.cl.GetFileURL(ctx, &filev1.GetFileURLRequest{FileId: strings.TrimSpace(fileID)})
	if err != nil {
		return "", err
	}
	return res.GetUrl(), nil
}

func (c *client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
