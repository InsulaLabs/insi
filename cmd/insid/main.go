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
	// misconfigured logger in raft bbolt backend
	// we use slog so this only silences the bbolt backend
	// to raft snapshot store that isn't directly used by us
	log.SetOutput(io.Discard)

	extensionLogger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	processExtension := process.NewExtension(extensionLogger.WithGroup("process"))

	remainingArgs := os.Args[1:]

	// The default config file path can be set here
	// It's passed to the runtime, which handles flag parsing for --config override.
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
