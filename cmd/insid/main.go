package main

import (
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/InsulaLabs/insi/plugins/chat"
	"github.com/InsulaLabs/insi/plugins/island"
	"github.com/InsulaLabs/insi/plugins/objects"
	"github.com/InsulaLabs/insi/plugins/static"
	"github.com/InsulaLabs/insi/plugins/status"
	"github.com/InsulaLabs/insi/runtime"
)

func main() {
	// misconfigured logger in raft bbolt backend
	// we use slog so this only silences the bbolt backend
	// to raft snapshot store that isn't directly used by us
	log.SetOutput(io.Discard)

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

	rt.WithPlugin(chat.New(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}).WithGroup("chat-plugin")),
	))

	rt.WithPlugin(island.New(slog.Default().WithGroup("island-plugin")))

	objectsDir := filepath.Join(rt.GetHomeDir(), "plugins", "objects")
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
