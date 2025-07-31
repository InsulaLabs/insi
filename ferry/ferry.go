package ferry

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/InsulaLabs/insi/client"
)

type Config struct {
	ApiKey     string
	Endpoints  []string // example: "db-0.insula.dev:443"
	SkipVerify bool
	Timeout    time.Duration
	Domain     string // If specified, will be used for client connections instead of endpoint host.
}

type Ferry struct {
	client *client.Client
	logger *slog.Logger
}

func New(logger *slog.Logger, cfg *Config) (*Ferry, error) {
	clientEndpoints := make([]client.Endpoint, len(cfg.Endpoints))
	for i, epStr := range cfg.Endpoints {
		clientEndpoints[i] = client.Endpoint{
			PublicBinding:  epStr,
			PrivateBinding: epStr,
			ClientDomain:   cfg.Domain,
			Logger:         logger,
		}
	}

	clientCfg := &client.Config{
		ConnectionType:         client.ConnectionTypeRandom,
		Endpoints:              clientEndpoints,
		ApiKey:                 cfg.ApiKey,
		SkipVerify:             cfg.SkipVerify,
		Timeout:                cfg.Timeout,
		Logger:                 logger,
		EnableLeaderStickiness: true,
	}

	c, err := client.NewClient(clientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create insi client: %w", err)
	}

	return &Ferry{
		client: c,
		logger: logger,
	}, nil
}

func (f *Ferry) Ping(attempts int, cooldown time.Duration) error {
	var lastErr error
	for i := 0; i < attempts; i++ {
		_, err := f.client.Ping()
		if err == nil {
			return nil
		}
		lastErr = err
		if i < attempts-1 {
			time.Sleep(cooldown)
		}
	}
	return fmt.Errorf(
		"ping failed after %d attempts: %w",
		attempts,
		lastErr)
}

func GetValueController[T any](f *Ferry, defaultT T) ValueController[T] {
	return NewValueController(defaultT, f.client, f.logger)
}

func GetCacheController[T any](f *Ferry, defaultT T) CacheController[T] {
	return NewCacheController(defaultT, f.client, f.logger)
}
