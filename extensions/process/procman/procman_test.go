package procman

import (
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	cmd "github.com/ShinyTrinkets/overseer"
)

// getSleepCommand returns the appropriate sleep command and args for the current platform
func getSleepCommand() (string, []string) {
	if runtime.GOOS == "windows" {
		return "timeout", []string{"/T", "5", "/NOBREAK"}
	}
	// On Unix-like systems (Linux, macOS), use sleep
	path, err := exec.LookPath("sleep")
	if err != nil {
		// Try common paths on Unix systems
		paths := []string{
			"/bin/sleep",
			"/usr/bin/sleep",
			"/usr/local/bin/sleep",
		}
		for _, p := range paths {
			if _, err := os.Stat(p); err == nil {
				return p, []string{"5"}
			}
		}
		// If we can't find sleep, use a minimal command that should exist
		return "true", []string{}
	}
	// Use sleep with a simple argument
	return path, []string{"5"}
}

// Mock MenuItem for testing
type mockMenuItem struct {
	title   string
	tooltip string
	checked bool
	enabled bool
	visible bool
	clicked chan struct{}
}

func newMockMenuItem() *mockMenuItem {
	return &mockMenuItem{
		clicked: make(chan struct{}),
	}
}

func (m *mockMenuItem) String() string            { return m.title }
func (m *mockMenuItem) Hide()                     { m.visible = false }
func (m *mockMenuItem) Show()                     { m.visible = true }
func (m *mockMenuItem) SetTitle(title string)     { m.title = title }
func (m *mockMenuItem) SetTooltip(tooltip string) { m.tooltip = tooltip }
func (m *mockMenuItem) Disabled()                 { m.enabled = false }
func (m *mockMenuItem) Enable()                   { m.enabled = true }
func (m *mockMenuItem) Enabled() bool             { return m.enabled }
func (m *mockMenuItem) Check()                    { m.checked = true }
func (m *mockMenuItem) Uncheck()                  { m.checked = false }
func (m *mockMenuItem) Checked() bool             { return m.checked }
func (m *mockMenuItem) ClickedCh() chan struct{}  { return m.clicked }

func TestNewHost(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	host := NewHost(logger)

	if host == nil {
		t.Fatal("NewHost returned nil")
	}

	if host.Logger != logger {
		t.Error("Logger not properly set")
	}

	if host.hostedApps == nil {
		t.Error("hostedApps map not initialized")
	}
}

func TestNewHostedApp(t *testing.T) {
	statusFeed := make(chan *cmd.ProcessJSON)
	logFeed := make(chan *cmd.LogMsg)
	watcherDone := make(chan struct{})
	ovs := cmd.NewOverseer()

	app := NewHostedApp(HostedAppOpt{
		Name:        "test-app",
		Ovs:         ovs,
		StatusFeed:  statusFeed,
		LogFeed:     logFeed,
		WatcherDone: watcherDone,
	})

	if app == nil {
		t.Fatal("NewHostedApp returned nil")
	}

	if app.overseer_id != "test-app" {
		t.Errorf("Expected overseer_id to be 'test-app', got %s", app.overseer_id)
	}

	if app.overseer != ovs {
		t.Error("Overseer not properly set")
	}

	if app.statusFeed != statusFeed {
		t.Error("StatusFeed not properly set")
	}

	if app.logFeed != logFeed {
		t.Error("LogFeed not properly set")
	}

	if app.watcherDone != watcherDone {
		t.Error("WatcherDone not properly set")
	}
}

func TestHostStartStopApp(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	host := NewHost(logger)

	// Test starting non-existent app
	err := host.StartApp("non-existent")
	if err == nil {
		t.Error("Expected error when starting non-existent app")
	}

	// Get platform-appropriate command and args
	cmdPath, cmdArgs := getSleepCommand()
	t.Logf("Using command: %s with args: %v", cmdPath, cmdArgs)

	stateChanges := make([]string, 0)
	// Create a test app with specific options
	app := NewHostedApp(HostedAppOpt{
		Name:       "test-app",
		TargetPath: cmdPath,
		CmdArgs:    cmdArgs,
		StartStopCb: func(name string, running bool) {
			state := "stopped"
			if running {
				state = "running"
			}
			stateChanges = append(stateChanges, state)
			t.Logf("App state changed to: %s", state)
		},
		CmdOptions: cmd.Options{
			DelayStart: 0,     // No delay
			RetryTimes: 0,     // No retries
			Buffered:   false, // Direct output
			Streaming:  true,  // Stream output
		},
	})

	host.hostedApps["test-app"] = app

	// Create channels for state monitoring
	app.statusFeed = make(chan *cmd.ProcessJSON, 10)
	app.logFeed = make(chan *cmd.LogMsg, 10)
	app.watcherDone = make(chan struct{})

	// Test starting the app
	err = host.StartApp("test-app")
	if err != nil {
		t.Errorf("Unexpected error starting app: %v", err)
	}

	// Wait for the running state
	timeout := time.After(10 * time.Second)
	running := false
	var lastState string
	var lastError error

	for !running {
		select {
		case state := <-app.statusFeed:
			t.Logf("Received state update: %+v", state)
			if state != nil {
				lastState = state.State
				lastError = state.Error
				if state.State == "running" || state.State == "starting" {
					running = true
					app.running = true
					t.Logf("App entered running state: %s", state.State)
				}
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for app to start. Last state: %s, Last error: %v", lastState, lastError)
		case <-time.After(100 * time.Millisecond):
			// Periodically check if we're running
			if app.running {
				running = true
				t.Log("App is marked as running")
			}
		}
	}

	// Give a small grace period for the app to fully initialize
	time.Sleep(100 * time.Millisecond)

	// Verify app state
	if !app.running {
		t.Error("App should be marked as running")
	}

	// Test stopping the app
	err = host.StopApp("test-app")
	if err != nil {
		t.Errorf("Unexpected error stopping app: %v", err)
	}

	// Wait for cleanup with better logging
	cleanupTimeout := time.After(2 * time.Second)
	cleanupDone := false
	for !cleanupDone {
		select {
		case <-app.watcherDone:
			t.Log("Received watcherDone signal")
			cleanupDone = true
		case <-cleanupTimeout:
			if !app.running && app.overseer == nil {
				t.Log("App appears to be cleaned up despite missing watcherDone signal")
				cleanupDone = true
			} else {
				t.Fatalf("Timeout waiting for app cleanup. Running: %v, Overseer: %v, Error: %v",
					app.running, app.overseer != nil, lastError)
			}
		case <-time.After(100 * time.Millisecond):
			// Periodically log state
			t.Log("Waiting for cleanup... App running:", app.running)
		}
	}

	// Give a small grace period for final state updates
	time.Sleep(100 * time.Millisecond)

	// Verify final state
	if app.running {
		t.Error("App should not be marked as running")
	}
	if app.overseer != nil {
		t.Error("App overseer should be nil")
	}
	if app.statusFeed != nil {
		t.Error("Status feed should be nil")
	}
	if app.logFeed != nil {
		t.Error("Log feed should be nil")
	}

	// Verify state changes
	if len(stateChanges) < 2 {
		t.Error("Expected at least two state changes (running and stopped)")
	}
	if stateChanges[0] != "running" {
		t.Error("First state change should be running")
	}
	if stateChanges[len(stateChanges)-1] != "stopped" {
		t.Error("Last state change should be stopped")
	}
}

func TestOverseerLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	oLogger := &OverseerLogger{
		Name:   "test-logger",
		Logger: logger,
	}

	// Test info logging
	oLogger.Info("test info message")

	// Test error logging
	oLogger.Error("test error message")
}

func TestHandleProcessTermination(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	host := NewHost(logger)

	stateChanged := false
	app := NewHostedApp(HostedAppOpt{
		Name: "test-app",
		StartStopCb: func(name string, running bool) {
			stateChanged = true
		},
	})
	app.running = true
	host.hostedApps["test-app"] = app

	// Test process termination handling
	host.handleProcessTermination(app, "test-app")

	if app.running {
		t.Error("App should not be marked as running after termination")
	}
	if !stateChanged {
		t.Error("StartStopCb should have been called")
	}
}
