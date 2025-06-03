package main

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/InsulaLabs/insi/plugins/etok"
	"github.com/InsulaLabs/insi/plugins/objects"
	"github.com/InsulaLabs/insi/plugins/static"
	"github.com/InsulaLabs/insi/plugins/status"
	"github.com/InsulaLabs/insi/runtime"
)

func main() {
	args := os.Args[1:]
	var staticPath string
	var remainingArgs []string

	// Check for "plugin" flags that the runtime doesn't know about (the reason they are plugins)
	for i := 0; i < len(args); i++ {
		if args[i] == "--static" {
			if i+1 < len(args) {
				staticPath = args[i+1]
				i++ // Skip the path argument
			} else {
				slog.Error("--static flag requires a path argument")
				os.Exit(1)
			}
		} else {
			remainingArgs = append(remainingArgs, args[i])
		}
	}

	// The default config file path can be set here
	// It's passed to the runtime, which handles flag parsing for --config override.
	rt, err := runtime.New(remainingArgs, "cluster.yaml")
	if err != nil {
		slog.Error("Failed to initialize runtime", "error", err)
		os.Exit(1)
	}

	// ------------------- Add Plugins -------------------

	rt.WithPlugin(status.New(slog.Default().WithGroup("status-plugin")))

	rt.WithPlugin(etok.New(slog.Default().WithGroup("etok-plugin")))

	objectsDir := filepath.Join(os.Getenv("HOME"), ".config", "insidb", "objects")
	if err := os.MkdirAll(objectsDir, 0755); err != nil {
		slog.Error("Failed to create objects directory", "error", err)
		os.Exit(1)
	}

	rt.WithPlugin(objects.New(slog.Default().WithGroup("objects-plugin"), objectsDir))

	if staticPath != "" {
		staticPlugin := static.New(slog.Default().WithGroup("static-plugin"), staticPath)
		rt.WithPlugin(staticPlugin)
		slog.Info("Static plugin enabled", "path", staticPath)
	}

	// ----------------- Start the runtime ----------------

	if err := rt.Run(); err != nil {
		slog.Error("Runtime exited with error", "error", err)
		os.Exit(1)
	}

	rt.Wait()
	slog.Info("Application exiting.")
}
