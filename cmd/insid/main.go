package main

import (
	"log/slog"
	"os"

	"github.com/InsulaLabs/insi/runtime"
)

func main() {
	// The default config file path can be set here
	// It's passed to the runtime, which handles flag parsing for --config override.
	rt, err := runtime.New(os.Args[1:], "cluster.yaml")
	if err != nil {
		// Use a basic logger for startup errors since runtime's logger might not be initialized.
		slog.Error("Failed to initialize runtime", "error", err)
		os.Exit(1)
	}

	if err := rt.Run(); err != nil {
		slog.Error("Runtime exited with error", "error", err)
		// Decide if this warrants a different exit code
		os.Exit(1) // Exit if Run returns an error, e.g. config issue
	}

	rt.Wait() // Wait for the runtime to be signaled to shut down.
	slog.Info("Application exiting.")
}
