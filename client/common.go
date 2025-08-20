package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"
)

const (
	DefaultInsiEndpointsList    = "https://red.insulalabs.io:443,https://green.insulalabs.io:443,https://blue.insulalabs.io:443"
	DefaultSelfHostEndpointList = "https://127.0.0.1:8443,https://127.0.0.1:8444,https://127.0.0.1:8445"
	DefaultInsiEndpointListVar  = "INSI_ENDPOINTS_LIST"
	DefaultInsiApiKeyVar        = "INSI_API_KEY"
	DefaultInsiSkipVerifyVar    = "INSI_SKIP_VERIFY"
	DefaultInsiIsLocalVar       = "INSI_IS_LOCAL"
)

/*
Common retry functions for rate limit handling and error backoff
*/
var CONFIG_MAX_VOID_RETRIES = 10

func WithRetriesVoid(ctx context.Context, logger *slog.Logger, fn func() error) error {
	_, err := WithRetries(ctx, CONFIG_MAX_VOID_RETRIES, logger, func() (any, error) {
		return nil, fn()
	})
	return err
}

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

/*
Simply load an insi client from the environment without the need
of a full config file
*/
func CreateClientFromEnv(logger *slog.Logger) (*Client, error) {
	apiKey := os.Getenv(DefaultInsiApiKeyVar)
	skipVerify := false
	if skipVerifyEnv := os.Getenv(DefaultInsiSkipVerifyVar); skipVerifyEnv == "true" {
		skipVerify = true
	}

	if apiKey == "" {
		return nil, fmt.Errorf("%s is not set", DefaultInsiApiKeyVar)
	}

	insiEndpointsList := os.Getenv(DefaultInsiEndpointListVar)
	if insiEndpointsList == "" {
		if isLocalEnv := os.Getenv(DefaultInsiIsLocalVar); isLocalEnv == "true" {
			insiEndpointsList = DefaultSelfHostEndpointList
		} else {
			insiEndpointsList = DefaultInsiEndpointsList
		}
	}

	insiEndpoints := strings.Split(insiEndpointsList, ",")

	extractDomain := func(addr string) string {
		if strings.HasPrefix(addr, "https://") {
			addr = strings.TrimPrefix(addr, "https://")
		} else if strings.HasPrefix(addr, "http://") {
			addr = strings.TrimPrefix(addr, "http://")
		}

		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return addr
		}
		return host
	}

	extractHostPort := func(addr string) string {
		if strings.HasPrefix(addr, "https://") {
			addr = strings.TrimPrefix(addr, "https://")
		} else if strings.HasPrefix(addr, "http://") {
			addr = strings.TrimPrefix(addr, "http://")
		}
		return addr
	}

	insiEndpointDomains := make([]string, len(insiEndpoints))
	for i, endpoint := range insiEndpoints {
		insiEndpointDomains[i] = extractDomain(endpoint)
	}

	endpoints := make([]Endpoint, len(insiEndpoints))

	for i, endpoint := range insiEndpoints {
		endpoints[i] = Endpoint{
			PublicBinding:  extractHostPort(endpoint),
			PrivateBinding: extractHostPort(endpoint),
			ClientDomain:   insiEndpointDomains[i],
		}
	}

	clientLogger := logger.WithGroup("client")

	c, err := NewClient(&Config{
		ConnectionType: ConnectionTypeDirect,
		Endpoints:      endpoints,
		ApiKey:         apiKey,
		SkipVerify:     skipVerify,
		Logger:         clientLogger,
	})

	pd, err := c.Ping()
	if err != nil {
		return nil, err
	}

	logger.Debug(
		"got ping from insi cluster",
		"connection-info", insiEndpoints,
		"skip-verify", skipVerify,
		"ping-data", pd,
	)

	return c, err
}
