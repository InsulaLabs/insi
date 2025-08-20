package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

func WithRetries[R any](ctx context.Context, retries int, logger *slog.Logger, fn func() (R, error)) (R, error) {

	retryCount := 0
	retryDelay := 100 * time.Millisecond

	for {
		if retryCount >= retries {
			var zero R
			return zero, fmt.Errorf("operation failed after %d retries", retryCount)
		}
		retryCount++

		result, err := fn()
		if err == nil {
			return result, nil
		}

		var rateLimitErr *ErrRateLimited
		if errors.As(err, &rateLimitErr) {
			logger.Debug("User operation rate limited, sleeping", "duration", rateLimitErr.RetryAfter)
			select {
			case <-time.After(rateLimitErr.RetryAfter):
				logger.Debug("Finished rate limit sleep, retrying operation.")
				continue
			case <-ctx.Done():
				logger.Error("Context cancelled during rate limit sleep", "error", ctx.Err())
				var zero R
				return zero, fmt.Errorf("operation cancelled during rate limit sleep: %w", ctx.Err())
			}
		}

		select {
		case <-time.After(retryDelay):
			retryDelay *= 2
			continue
		case <-ctx.Done():
			var zero R
			return zero, fmt.Errorf("operation cancelled during error backoff: %w", ctx.Err())
		}
	}
}

var CONFIG_MAX_VOID_RETRIES = 10

func WithRetriesVoid(ctx context.Context, logger *slog.Logger, fn func() error) error {
	_, err := WithRetries(ctx, CONFIG_MAX_VOID_RETRIES, logger, func() (any, error) {
		return nil, fn()
	})
	return err
}
