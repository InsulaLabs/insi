package ferry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/InsulaLabs/insi/client"
)

type BlobController interface {
	PushScope(prefix string) error
	PopScope() error
	GetPrefix() string

	Upload(ctx context.Context, key string, reader io.Reader, filename string) error
	Download(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	IterateByPrefix(ctx context.Context, prefix string, offset, limit int) ([]string, error)
}

type bcImpl struct {
	client *client.Client
	logger *slog.Logger

	scopes       []string
	prefix       string
	scopeChanged atomic.Bool
}

func NewBlobController(c *client.Client, logger *slog.Logger) BlobController {
	return &bcImpl{
		client: c,
		logger: logger.WithGroup("blob_controller"),
	}
}

func (bc *bcImpl) buildPrefix(key string) string {
	if len(bc.scopes) == 0 {
		return key
	}
	if !bc.scopeChanged.Load() {
		return fmt.Sprintf("%s:%s", bc.prefix, key)
	}

	bc.prefix = strings.Join(bc.scopes, ":")
	bc.scopeChanged.Store(false)
	return fmt.Sprintf("%s:%s", bc.prefix, key)
}

func (bc *bcImpl) GetPrefix() string {
	return bc.prefix
}

func (bc *bcImpl) PushScope(prefix string) error {
	if prefix == "" {
		return ErrEmptyScopePush
	}

	bc.scopes = append(bc.scopes, prefix)
	bc.scopeChanged.Store(true)
	return nil
}

func (bc *bcImpl) PopScope() error {
	if len(bc.scopes) == 0 {
		return ErrEndScope
	}

	bc.scopes = bc.scopes[:len(bc.scopes)-1]
	bc.scopeChanged.Store(true)
	return nil
}

func (bc *bcImpl) Upload(ctx context.Context, key string, reader io.Reader, filename string) error {
	err := client.WithRetriesVoid(ctx, bc.logger, func() error {
		fullKey := bc.buildPrefix(key)
		return bc.client.UploadBlob(ctx, fullKey, reader, filename)
	})

	return translateError(err)
}

func (bc *bcImpl) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	var result io.ReadCloser

	err := client.WithRetriesVoid(ctx, bc.logger, func() error {
		fullKey := bc.buildPrefix(key)
		rc, err := bc.client.GetBlob(ctx, fullKey)
		if err != nil {
			return err
		}
		result = rc
		return nil
	})

	return result, translateError(err)
}

func (bc *bcImpl) Delete(ctx context.Context, key string) error {
	err := client.WithRetriesVoid(ctx, bc.logger, func() error {
		fullKey := bc.buildPrefix(key)
		return bc.client.DeleteBlob(fullKey)
	})

	return translateError(err)
}

func (bc *bcImpl) IterateByPrefix(ctx context.Context, prefix string, offset, limit int) ([]string, error) {
	var result []string

	err := client.WithRetriesVoid(ctx, bc.logger, func() error {
		fullPrefix := bc.buildPrefix(prefix)
		keys, err := bc.client.IterateBlobKeysByPrefix(fullPrefix, offset, limit)
		if err != nil {
			return err
		}
		result = keys
		return nil
	})

	return result, translateError(err)
}
