package main

import (
	"context"
	"io"
	"log"
	"log/slog"
	"os"

	"github.com/InsulaLabs/insi/pkg/extensions/process"
	"github.com/InsulaLabs/insi/pkg/runtime"
	"github.com/google/uuid"
)

var ctx = context.Background()

func init() {
	uuid.EnableRandPool()
}

func main() {
	log.SetOutput(io.Discard)

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	processExtension := process.NewExtension(logger.WithGroup("process"))

	remainingArgs := os.Args[1:]

	rt, err := runtime.New(ctx, remainingArgs, "cluster.yaml")
	if err != nil {
		slog.Error("Failed to initialize runtime", "error", err)
		os.Exit(1)
	}

	rt = rt.WithExtension(processExtension)

	if err := rt.Run(); err != nil {
		slog.Error("Runtime exited with error", "error", err)
		os.Exit(1)
	}

	rt.Wait()
	slog.Info("Application exiting.")
}
