package app

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/internal/db/core"
	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/google/uuid"
)

type Session struct {
	sessionID string
	userID    string

	history       []string
	historyIndex  int
	currentBuffer string
	inHistoryMode bool

	config         SessionConfig
	startTimestamp time.Time

	userFWI           fwi.Entity
	extensionControls []core.ExtensionControl

	sessionRuntimeCtx context.Context
	virtualFileSystem sessionVFS

	applications AppMap
	appHelpText  string
}

type SessionConfig struct {
	Logger               *slog.Logger
	UserID               string
	ActiveCursorSymbol   string
	InactiveCursorSymbol string
	Prompt               string
	UserFWI              fwi.Entity
}

func NewSession(ctx context.Context, config SessionConfig, extensionControls []core.ExtensionControl, applications AppMap) *Session {

	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	sb := strings.Builder{}
	for appName, appConstructor := range applications {
		app := appConstructor()
		appHelpText := app.GetHelpText()
		appHelpText += "  " + appName + " - " + appHelpText + "\n"
		sb.WriteString(appHelpText)
	}

	session := &Session{
		sessionID:         uuid.New().String(),
		userID:            config.UserID,
		history:           []string{},
		historyIndex:      -1,
		inHistoryMode:     false,
		config:            config,
		startTimestamp:    time.Now(),
		userFWI:           config.UserFWI,
		extensionControls: extensionControls,
		sessionRuntimeCtx: ctx,
		virtualFileSystem: sessionVFS{activeDirectory: "/", fs: config.UserFWI.GetFS()},
		applications:      applications,
		appHelpText:       sb.String(),
	}

	// Ensure the root directory exists
	if err := ensureDirectoryExists(ctx, session.virtualFileSystem.fs, "/"); err != nil {
		// Log the error but don't fail session creation
		config.Logger.Warn("Failed to ensure root directory exists", "error", err)
	}

	return session
}

func (s *Session) AddToHistory(cmd string) {
	if cmd != "" {
		s.history = append(s.history, cmd)
		s.historyIndex = len(s.history)
		s.inHistoryMode = false
	}
}

func (s *Session) StartHistoryNavigation(currentBuffer string) {
	if !s.inHistoryMode {
		s.currentBuffer = currentBuffer
		s.inHistoryMode = true
		s.historyIndex = len(s.history)
	}
}

func (s *Session) IsInHistoryMode() bool {
	return s.inHistoryMode
}

func (s *Session) NavigateHistory(up bool) string {
	if len(s.history) == 0 {
		return ""
	}

	if up {
		if s.historyIndex > 0 {
			s.historyIndex--
			return s.history[s.historyIndex]
		}
	} else {
		if s.historyIndex < len(s.history)-1 {
			s.historyIndex++
			return s.history[s.historyIndex]
		} else {
			s.historyIndex = len(s.history)
			s.inHistoryMode = false
			return s.currentBuffer
		}
	}

	if s.historyIndex >= 0 && s.historyIndex < len(s.history) {
		return s.history[s.historyIndex]
	}

	return s.currentBuffer
}

func (s *Session) GetHistory() []string {
	return s.history
}

func (s *Session) GetActiveCursorSymbol() string {
	return s.config.ActiveCursorSymbol
}

func (s *Session) GetInactiveCursorSymbol() string {
	return s.config.InactiveCursorSymbol
}

func (s *Session) UserUptime() time.Duration {
	return time.Since(s.startTimestamp)
}

func (s *Session) GetUserID() string {
	return s.config.UserID
}

func (s *Session) GetPrompt() string {
	return s.config.Prompt
}

func (s *Session) GetFWI() fwi.Entity {
	return s.userFWI
}

/*
	Data helpers
*/

func (s *Session) GetLoadedExtensionNames() []string {
	extensionNames := make([]string, len(s.extensionControls))
	for i, extension := range s.extensionControls {
		extensionNames[i] = extension.CommandName()
	}
	return extensionNames
}

/*
	VFS Helper Methods
*/

// ensureDirectoryExists checks if a directory exists and creates it if it doesn't
func ensureDirectoryExists(ctx context.Context, fs fwi.FS, path string) error {
	_, err := fs.Stat(ctx, path)
	if err != nil {
		// Directory doesn't exist, try to create it
		return fs.Mkdir(ctx, path)
	}
	return nil
}

func (s *Session) GetCurrentDirectory() string {
	return s.virtualFileSystem.activeDirectory
}

func (s *Session) ChangeDirectory(ctx context.Context, newPath string) error {
	// Resolve the path (handle relative paths)
	resolvedPath := s.ResolvePath(newPath)

	// Ensure the directory exists (create it if it doesn't)
	if err := ensureDirectoryExists(ctx, s.virtualFileSystem.fs, resolvedPath); err != nil {
		return fmt.Errorf("failed to access directory %s: %w", newPath, err)
	}

	// Verify it's actually a directory
	info, err := s.virtualFileSystem.fs.Stat(ctx, resolvedPath)
	if err != nil {
		return fmt.Errorf("directory not accessible: %s", newPath)
	}

	if !info.IsDir() {
		return fmt.Errorf("not a directory: %s", newPath)
	}

	s.virtualFileSystem.activeDirectory = resolvedPath
	return nil
}

func (s *Session) ResolvePath(path string) string {
	if strings.HasPrefix(path, "/") {
		return path
	}

	// Handle relative path
	current := s.virtualFileSystem.activeDirectory
	if current == "/" {
		return "/" + path
	}

	return filepath.Join(current, path)
}

func (s *Session) BuildHelpText() string {
	var helpText string

	helpText += "Available Commands:\n\n"

	helpText += "Built-in Commands:\n"
	helpText += "  exit - Exit the session\n"
	helpText += "  help - Display this help message\n\n"

	helpText += "File System Commands:\n"
	helpText += "  pwd   - Show current directory\n"
	helpText += "  cd    - Change directory\n"
	helpText += "  ls    - List directory contents\n"
	helpText += "  cat   - Display file contents\n"
	helpText += "  mkdir - Create directory\n"
	helpText += "  touch - Create empty file\n"
	helpText += "  rm    - Remove files and directories\n\n"

	if len(s.extensionControls) > 0 {
		helpText += "Extension Commands:\n"
		for _, extension := range s.extensionControls {
			helpText += "  " + extension.CommandName() + " - " + extension.GetHelpText() + "\n"
		}
	} else {
		helpText += "No extensions loaded.\n"
	}

	helpText += "\nAvailable Applications:\n\n"
	helpText += s.appHelpText

	return helpText
}

func (s *Session) GetSessionRuntimeCtx() context.Context {
	return s.sessionRuntimeCtx
}
