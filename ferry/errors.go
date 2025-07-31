package ferry

import (
	"errors"
	"fmt"

	"github.com/InsulaLabs/insi/client"
)

var (
	ErrEmptyScopePush = errors.New("empty scope push")
	ErrEndScope       = errors.New("end scope")

	ErrKeyNotFound             = errors.New("key not found")
	ErrConflict                = errors.New("operation failed due to a conflict")
	ErrRateLimited             = errors.New("rate limited")
	ErrDiskLimitExceeded       = errors.New("disk limit exceeded")
	ErrMemoryLimitExceeded     = errors.New("memory limit exceeded")
	ErrEventsLimitExceeded     = errors.New("events limit exceeded")
	ErrSubscriberLimitExceeded = errors.New("subscriber limit exceeded")
	ErrAPIKeyNotFound          = errors.New("api key not found")
)

func translateError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, client.ErrKeyNotFound) {
		return ErrKeyNotFound
	}
	if errors.Is(err, client.ErrConflict) {
		return ErrConflict
	}
	if errors.Is(err, client.ErrAPIKeyNotFound) {
		return ErrAPIKeyNotFound
	}

	var rateLimitErr *client.ErrRateLimited
	if errors.As(err, &rateLimitErr) {
		return fmt.Errorf(
			"%w: %s. Try again in %v. (Limit: %d, Burst: %d)",
			ErrRateLimited,
			rateLimitErr.Message,
			rateLimitErr.RetryAfter,
			rateLimitErr.Limit,
			rateLimitErr.Burst,
		)
	}

	var diskLimitErr *client.ErrDiskLimitExceeded
	if errors.As(err, &diskLimitErr) {
		return fmt.Errorf(
			"%w: %s (current: %d, limit: %d)",
			ErrDiskLimitExceeded,
			diskLimitErr.Message,
			diskLimitErr.CurrentUsage,
			diskLimitErr.Limit,
		)
	}

	var memoryLimitErr *client.ErrMemoryLimitExceeded
	if errors.As(err, &memoryLimitErr) {
		return fmt.Errorf(
			"%w: %s (current: %d, limit: %d)",
			ErrMemoryLimitExceeded,
			memoryLimitErr.Message,
			memoryLimitErr.CurrentUsage,
			memoryLimitErr.Limit,
		)
	}

	var eventsLimitErr *client.ErrEventsLimitExceeded
	if errors.As(err, &eventsLimitErr) {
		return fmt.Errorf(
			"%w: %s (current: %d, limit: %d)",
			ErrEventsLimitExceeded,
			eventsLimitErr.Message,
			eventsLimitErr.CurrentUsage,
			eventsLimitErr.Limit,
		)
	}

	var subscriberLimitErr *client.ErrSubscriberLimitExceeded
	if errors.As(err, &subscriberLimitErr) {
		return fmt.Errorf(
			"%w: %s",
			ErrSubscriberLimitExceeded,
			subscriberLimitErr.Message,
		)
	}

	return err
}
