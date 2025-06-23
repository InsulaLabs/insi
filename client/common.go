package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

func withRetries[R any](ctx context.Context, logger *slog.Logger, fn func() (R, error)) (R, error) {
	for {
		result, err := fn()
		if err == nil {
			return result, nil // Success
		}

		var rateLimitErr *ErrRateLimited
		if errors.As(err, &rateLimitErr) {
			logger.Warn("User operation rate limited, sleeping", "duration", rateLimitErr.RetryAfter)
			select {
			case <-time.After(rateLimitErr.RetryAfter):
				logger.Debug("Finished rate limit sleep, retrying operation.")
				continue // Slept, continue to retry
			case <-ctx.Done():
				logger.Error("Context cancelled during rate limit sleep", "error", ctx.Err())
				var zero R
				return zero, fmt.Errorf("operation cancelled during rate limit sleep: %w", ctx.Err())
			}
		}

		var zero R
		return zero, err
	}
}

func withRetriesVoid(ctx context.Context, logger *slog.Logger, fn func() error) error {
	_, err := withRetries(ctx, logger, func() (any, error) {
		return nil, fn()
	})
	return err
}
