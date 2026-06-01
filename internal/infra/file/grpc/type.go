package grpc

import "context"

// FileURLClient resolves public file URLs for user-service read-model
// enrichment.
type FileURLClient interface {
	GetFileURL(ctx context.Context, fileID string) (string, error)
	Close() error
}
