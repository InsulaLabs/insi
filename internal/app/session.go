package app

import (
	"context"
	"log/slog"
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
}

type SessionConfig struct {
	Logger               *slog.Logger
	UserID               string
	ActiveCursorSymbol   string
	InactiveCursorSymbol string
	Prompt               string
	UserFWI              fwi.Entity
}

func NewSession(ctx context.Context, config SessionConfig, extensionControls []core.ExtensionControl) *Session {

	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	return &Session{
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
	}
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

func (s *Session) BuildHelpText() string {
	var helpText string

	helpText += "Available Commands:\n\n"

	helpText += "Built-in Commands:\n"
	helpText += "  exit - Exit the session\n"
	helpText += "  help - Display this help message\n\n"

	if len(s.extensionControls) > 0 {
		helpText += "Extension Commands:\n"
		for _, extension := range s.extensionControls {
			helpText += "  " + extension.CommandName() + " - " + extension.GetHelpText() + "\n"
		}
	} else {
		helpText += "No extensions loaded.\n"
	}

	return helpText
}
