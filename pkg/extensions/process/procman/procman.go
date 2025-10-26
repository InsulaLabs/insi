// Package main provides a process management system that safely handles the lifecycle
// of long-running processes. It uses the overseer library for process supervision
// and provides callback-based state management.
//
// Usage:
//
//	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
//	host := NewHost(logger)
//
//	// Create and register an app with state change callback
//	app := NewHostedApp(HostedAppOpt{
//	    Name:       "my-app",
//	    TargetPath: "/path/to/executable",
//	    CmdArgs:    []string{"--arg1", "value1"},
//	    CmdOptions: DefaultOptions(),
//	    StartStopCb: func(name string, running bool) {
//	        log.Printf("App %s is now %s", name, map[bool]string{true:"running",false:"stopped"}[running])
//	    },
//	})
//
//	// Start the app (non-blocking)
//	if err := host.StartApp("my-app"); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Later, stop the app safely
//	if err := host.StopApp("my-app"); err != nil {
//	    log.Fatal(err)
//	}
//
// The package handles process state management, graceful shutdowns (SIGTERM),
// and cleanup of resources. It is safe for cross-platform use and provides
// detailed logging of process lifecycle events.
package procman

import (
	"fmt"
	"log/slog"
	"sync"
	"syscall"
	"time"

	cmd "github.com/ShinyTrinkets/overseer"
)

type Host struct {
	Logger     *slog.Logger
	hostedApps map[string]*HostedApp
}

type HostedApp struct {
	running     bool
	overseer_id string
	startStopCb func(name string, running bool)
	targetPath  string
	cmdArgs     []string
	cmdOptions  cmd.Options
	overseer    *cmd.Overseer
	statusFeed  chan *cmd.ProcessJSON
	logFeed     chan *cmd.LogMsg
	watcherDone chan struct{}
	channelSize int
}

func NewHost(logger *slog.Logger) *Host {
	return &Host{
		Logger:     logger,
		hostedApps: map[string]*HostedApp{},
	}
}

func (h *Host) WithApp(app *HostedApp) *Host {
	h.hostedApps[app.overseer_id] = app
	return h
}

type HostedAppOpt struct {
	Name        string
	Ovs         *cmd.Overseer
	StartStopCb func(name string, running bool)
	StatusFeed  chan *cmd.ProcessJSON
	LogFeed     chan *cmd.LogMsg
	WatcherDone chan struct{}
	TargetPath  string
	CmdArgs     []string
	CmdOptions  cmd.Options
	ChannelSize int
}

func DefaultOptions() cmd.Options {
	return cmd.Options{
		DelayStart: 0,     // No startup delay
		RetryTimes: 0,     // No automatic retries
		Buffered:   false, // Direct output
		Streaming:  true,  // Stream output
	}
}

func NewHostedApp(opts HostedAppOpt) *HostedApp {
	options := DefaultOptions()

	if opts.CmdOptions.DelayStart != 0 {
		options.DelayStart = opts.CmdOptions.DelayStart
	}
	if opts.CmdOptions.RetryTimes != 0 {
		options.RetryTimes = opts.CmdOptions.RetryTimes
	}
	options.Buffered = opts.CmdOptions.Buffered
	options.Streaming = opts.CmdOptions.Streaming

	if opts.ChannelSize <= 0 {
		opts.ChannelSize = 100
	}

	return &HostedApp{
		overseer_id: opts.Name,
		overseer:    opts.Ovs,
		startStopCb: opts.StartStopCb,
		statusFeed:  opts.StatusFeed,
		watcherDone: opts.WatcherDone,
		logFeed:     opts.LogFeed,
		targetPath:  opts.TargetPath,
		cmdArgs:     opts.CmdArgs,
		cmdOptions:  options,
		channelSize: opts.ChannelSize,
	}
}

type OverseerLogger struct {
	Name   string
	Logger *slog.Logger
}

func (l *OverseerLogger) Info(msg string, v ...interface{}) {
	l.Logger.Info(msg, v...)
}

func (l *OverseerLogger) Error(msg string, v ...interface{}) {
	l.Logger.Error(msg, v...)
}

func (h *Host) StartApp(name string) error {
	app, ok := h.hostedApps[name]
	if !ok {
		return fmt.Errorf("app not found")
	}

	if app.overseer != nil {
		if err := h.StopApp(name); err != nil {
			h.Logger.Error("failed to clean up previous overseer", "error", err)
		}
		if app.watcherDone != nil {
			select {
			case <-app.watcherDone:
				h.Logger.Info("watcher done signal received", "app", name)
			case <-time.After(2 * time.Second):
				h.Logger.Warn("timeout waiting for watcher done signal, proceeding anyway", "app", name)
			}
		}
	}

	app.overseer = cmd.NewOverseer()

	app.statusFeed = make(chan *cmd.ProcessJSON, app.channelSize)
	app.logFeed = make(chan *cmd.LogMsg, app.channelSize)
	app.watcherDone = make(chan struct{})

	app.overseer.Add(app.overseer_id, app.targetPath, app.cmdArgs, app.cmdOptions)

	app.overseer.WatchState(app.statusFeed)
	app.overseer.WatchLogs(app.logFeed)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		h.watchAppState(name)
		wg.Done()
	}()

	go func() {
		h.Logger.Info("starting supervision", "app", name)
		app.overseer.Supervise(app.overseer_id)
		h.Logger.Info("supervision ended, process terminated", "app", name)
		h.handleProcessTermination(app, name)
		wg.Wait()
	}()

	timeout := time.After(2 * time.Second)
loop:
	for !app.running {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for app to start")
		case <-time.After(50 * time.Millisecond):
			if app.running {
				break loop
			}
		}
	}

	return nil
}

func (h *Host) handleProcessTermination(app *HostedApp, name string) {
	if app == nil {
		h.Logger.Error("nil app in handleProcessTermination", "name", name)
		return
	}

	h.Logger.Info("updating state for terminated app", "name", name)
	app.running = false
	if app.startStopCb != nil {
		app.startStopCb(name, false)
	}
}

func (h *Host) watchAppState(name string) {
	h.Logger.Info("starting state watcher for", "name", name)

	app, ok := h.hostedApps[name]
	if !ok {
		h.Logger.Error("attempted to watch non-existent app", "name", name)
		return
	}

	defer func() {
		h.Logger.Info("state watcher ended", "app", name)
		if app.watcherDone != nil {
			select {
			case app.watcherDone <- struct{}{}:
				h.Logger.Info("sent watcher done signal", "app", name)
			default:
				h.Logger.Info("watcher done channel unavailable", "app", name)
			}
		}
	}()

	for state := range app.statusFeed {
		if state == nil {
			continue
		}

		if state.ID != app.overseer_id {
			continue
		}

		h.Logger.Info("process state change",
			"app", name,
			"state", state.State,
			"pid", state.PID,
			"exit_code", state.ExitCode,
			"error", state.Error)

		switch state.State {
		case "starting", "running":
			app.running = true
			if app.startStopCb != nil {
				app.startStopCb(name, true)
			}
		case "stopped", "exited", "killed":
			app.running = false
			if app.startStopCb != nil {
				app.startStopCb(name, false)
			}
		}
	}
}

func (h *Host) GetApps() map[string]*HostedApp {
	return h.hostedApps
}

func (app *HostedApp) IsRunning() bool {
	return app.running
}

func (app *HostedApp) GetName() string {
	return app.overseer_id
}

func (h *Host) StopApp(name string) error {
	if name == "*" {
		for _, app := range h.hostedApps {
			h.Logger.Info("stopping app", "name", app.overseer_id)
			if err := h.internalStopApp(app.overseer_id); err != nil {
				h.Logger.Error("failed to stop app", "error", err)
				continue
			}
		}
		return nil
	}
	return h.internalStopApp(name)
}

func (h *Host) internalStopApp(name string) error {
	app, ok := h.hostedApps[name]
	if !ok {
		return fmt.Errorf("app not found")
	}

	if app.overseer == nil {
		return fmt.Errorf("app overseer is nil")
	}

	h.Logger.Info("attempting to stop app", "name", name, "id", app.overseer_id)

	// Create a new watcherDone channel if needed
	if app.watcherDone == nil {
		app.watcherDone = make(chan struct{})
	}

	// First signal the process to stop
	if err := app.overseer.Signal(app.overseer_id, syscall.SIGTERM); err != nil {
		h.Logger.Error("failed to send SIGTERM", "error", err)
	}

	// Give the process a moment to handle the signal
	time.Sleep(100 * time.Millisecond)

	// Stop the overseer - this triggers the cleanup sequence
	if err := app.overseer.Stop(app.overseer_id); err != nil {
		h.Logger.Error("failed to stop overseer", "error", err)
	}

	h.Logger.Info("stop command sent successfully", "name", name)

	// Create a done channel for synchronization
	done := make(chan struct{})

	// Start a goroutine to handle cleanup after watcher is done
	go func() {
		defer close(done)

		// Wait for watcher completion
		select {
		case <-app.watcherDone:
			h.Logger.Info("received watcher completion signal", "app", name)
		case <-time.After(1 * time.Second):
			h.Logger.Warn("timed out waiting for watcher completion", "app", name)
		}

		// Now safe to clean up channels
		if app.statusFeed != nil {
			app.overseer.UnWatchState(app.statusFeed)
			close(app.statusFeed)
			app.statusFeed = nil
		}
		if app.logFeed != nil {
			app.overseer.UnWatchLogs(app.logFeed)
			close(app.logFeed)
			app.logFeed = nil
		}

		// Remove from overseer
		if removed := app.overseer.Remove(app.overseer_id); !removed {
			h.Logger.Error("failed to remove app from overseer", "name", name)
		}

		app.overseer = nil
		app.running = false
	}()

	// Wait for cleanup to complete
	<-done
	h.Logger.Info("app cleanup completed", "name", name)
	return nil
}
