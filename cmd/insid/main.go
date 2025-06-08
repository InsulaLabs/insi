package main

import (
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/InsulaLabs/insi/runtime"
	"github.com/InsulaLabs/insi/service/objects"
	"github.com/InsulaLabs/insi/service/status"
)

func main() {
	// misconfigured logger in raft bbolt backend
	// we use slog so this only silences the bbolt backend
	// to raft snapshot store that isn't directly used by us
	log.SetOutput(io.Discard)

	remainingArgs := os.Args[1:]

	// The default config file path can be set here
	// It's passed to the runtime, which handles flag parsing for --config override.
	rt, err := runtime.New(remainingArgs, "cluster.yaml")
	if err != nil {
		slog.Error("Failed to initialize runtime", "error", err)
		os.Exit(1)
	}

	// ------------------- Add Services -------------------

	rt.WithService(status.New(slog.Default().WithGroup("status-service")))

	objectsDir := filepath.Join(rt.GetHomeDir(), "services", "objects")
	if err := os.MkdirAll(objectsDir, 0755); err != nil {
		slog.Error("Failed to create objects directory", "error", err)
		os.Exit(1)
	}

	rt.WithService(objects.New(slog.Default().WithGroup("objects-service"), objectsDir))

	// ----------------- Start the runtime ----------------

	if err := rt.Run(); err != nil {
		slog.Error("Runtime exited with error", "error", err)
		os.Exit(1)
	}

	rt.Wait()
	slog.Info("Application exiting.")
}
