package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/InsulaLabs/insi/pkg/client"
	"github.com/fatih/color"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	displayEnvVars()
	c, err := client.CreateClientFromEnv(logger)
	if err != nil {
		logger.Error("failed to get insi client", "error", err)
		os.Exit(1)
	}
	pd, err := c.Ping()
	if err != nil {
		logger.Error("failed to ping insi", "error", err)
		os.Exit(1)
	}
	logger.Debug("got ping from insi cluster", "ping-data", pd)
}

func displayEnvVars() {
	vars := map[string]string{
		client.DefaultInsiApiKeyVar:       os.Getenv(client.DefaultInsiApiKeyVar),
		client.DefaultInsiEndpointListVar: os.Getenv(client.DefaultInsiEndpointListVar),
		client.DefaultInsiSkipVerifyVar:   os.Getenv(client.DefaultInsiSkipVerifyVar),
		client.DefaultInsiIsLocalVar:      os.Getenv(client.DefaultInsiIsLocalVar),
	}
	if vars[client.DefaultInsiApiKeyVar] != "" {
		vars[client.DefaultInsiApiKeyVar] = fmt.Sprintf("%s..", vars[client.DefaultInsiApiKeyVar][:16])
	}
	for key, value := range vars {
		fmt.Printf("%s: %s\n", color.YellowString(key), color.CyanString(value))
	}
}
