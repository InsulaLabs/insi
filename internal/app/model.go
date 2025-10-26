package app

import (
	"context"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/internal/db/core"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/log"
)

type tickMsg time.Time

type commandOutputMsg struct {
	output string
	isErr  bool
}

type displayEntryType int

const (
	displayEntryCommand displayEntryType = iota
	displayEntryOutput
)

type displayEntry struct {
	entryType displayEntryType
	content   string
	isErr     bool
}

type CLICmdHandler func(session *Session, command string, args []string) tea.Cmd

type Model struct {
	session    *Session
	buffer     string
	cursor     int
	quitting   bool
	cursorOn   bool
	currentApp App
	windowSize tea.WindowSizeMsg

	applications AppMap

	commands map[string]CLICmdHandler

	displayHistory []displayEntry

	ctx context.Context
}

type ReplConfig struct {
	SessionConfig SessionConfig
}

func New(ctx context.Context, config ReplConfig, applications AppMap, extensionControls []core.ExtensionControl) Model {
	session := NewSession(ctx, config.SessionConfig, extensionControls, applications)

	return Model{
		session:      session,
		ctx:          ctx,
		buffer:       "",
		cursor:       0,
		cursorOn:     true,
		applications: applications,
		commands:     getCommandMap(ctx, extensionControls),
	}
}

func (m Model) Init() tea.Cmd {
	return tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// Store window size for newly launched apps
	if windowMsg, ok := msg.(tea.WindowSizeMsg); ok {
		m.windowSize = windowMsg
	}

	if m.currentApp != nil {
		newApp, cmd := m.currentApp.Update(msg)
		if newApp == nil {
			m.currentApp = nil
			return m, tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
				return tickMsg(t)
			})
		}
		m.currentApp = newApp
		return m, cmd
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			if m.buffer != "" {
				m.displayHistory = append(m.displayHistory, displayEntry{
					entryType: displayEntryCommand,
					content:   m.buffer + "^C",
				})
			}
			m.buffer = ""
			m.cursor = 0
			return m, nil
		case tea.KeyCtrlD:
			m.quitting = true
			return m, tea.Quit
		case tea.KeyEnter:
			command := strings.TrimSpace(m.buffer)
			m.buffer = ""
			m.cursor = 0
			if command != "" {
				log.Info("Command received", "command", command)

				m.session.AddToHistory(command)

				m.displayHistory = append(m.displayHistory, displayEntry{
					entryType: displayEntryCommand,
					content:   command,
				})

				cmd, args := m.splitCommandIntoCommandAndArgs(command)
				if app, ok := m.applications[cmd]; ok {
					return m, m.LaunchApp(app(), args)
				}

				if cmdHandler, ok := m.commands[cmd]; ok {
					return m, cmdHandler(m.session, cmd, args)
				}
			}
			return m, nil
		case tea.KeyBackspace:
			if m.cursor > 0 {
				m.buffer = m.buffer[:m.cursor-1] + m.buffer[m.cursor:]
				m.cursor--
			}
			return m, nil
		case tea.KeyLeft:
			if m.cursor > 0 {
				m.cursor--
			}
			return m, nil
		case tea.KeyRight:
			if m.cursor < len(m.buffer) {
				m.cursor++
			}
			return m, nil
		case tea.KeyUp:
			m.session.StartHistoryNavigation(m.buffer)
			if historyCmd := m.session.NavigateHistory(true); historyCmd != "" || m.session.IsInHistoryMode() {
				m.buffer = historyCmd
				m.cursor = len(m.buffer)
			}
			return m, nil
		case tea.KeyDown:
			if m.session.IsInHistoryMode() {
				if historyCmd := m.session.NavigateHistory(false); true {
					m.buffer = historyCmd
					m.cursor = len(m.buffer)
				}
			}
			return m, nil
		case tea.KeySpace:
			m.buffer = m.buffer[:m.cursor] + " " + m.buffer[m.cursor:]
			m.cursor++
			return m, nil
		default:
			if msg.Type == tea.KeyRunes {
				text := msg.String()

				if strings.HasPrefix(text, "[") && strings.HasSuffix(text, "]") && len(text) > 2 {
					text = text[1 : len(text)-1]
				}

				text = strings.ReplaceAll(text, "\r", "")
				text = strings.ReplaceAll(text, "\n", " ")
				text = strings.ReplaceAll(text, "\t", " ")
				m.buffer = m.buffer[:m.cursor] + text + m.buffer[m.cursor:]
				m.cursor += len(text)
			}
			return m, nil
		}
	case tickMsg:
		m.cursorOn = !m.cursorOn
		return m, tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
			return tickMsg(t)
		})
	case commandOutputMsg:
		m.displayHistory = append(m.displayHistory, displayEntry{
			entryType: displayEntryOutput,
			content:   msg.output,
			isErr:     msg.isErr,
		})
		return m, nil
	}
	return m, nil
}

// View renders the model
func (m Model) View() string {
	if m.quitting {
		return "Goodbye!\n"
	}

	// If we have an active app, show its view
	if m.currentApp != nil {
		return m.currentApp.View()
	}

	var b strings.Builder

	b.WriteString("REPL v0.1.0\n")
	b.WriteString("Type 'help' for available commands.\n")
	b.WriteString("Type 'exit' or press Ctrl+C/Ctrl+D to quit.\n\n")

	for _, entry := range m.displayHistory {
		switch entry.entryType {
		case displayEntryCommand:
			b.WriteString(m.session.GetPrompt())
			b.WriteString(entry.content)
			b.WriteString("\n")
		case displayEntryOutput:
			b.WriteString(entry.content)
			if !strings.HasSuffix(entry.content, "\n") {
				b.WriteString("\n")
			}
		}
	}

	b.WriteString(m.session.GetPrompt())

	beforeCursor := m.buffer[:m.cursor]
	afterCursor := m.buffer[m.cursor:]

	b.WriteString(beforeCursor)

	if m.cursorOn {
		b.WriteString(m.session.GetActiveCursorSymbol())
	} else {
		b.WriteString(m.session.GetInactiveCursorSymbol())
	}

	b.WriteString(afterCursor)
	b.WriteString("\n")

	return b.String()
}

func (m *Model) LaunchApp(app App, args []string) tea.Cmd {
	m.buffer = ""
	m.cursor = 0
	m.currentApp = app
	// Launch the app and send the current window size so it sizes immediately
	appInitCmd := app.Init(m.session, args)
	if m.windowSize.Width > 0 && m.windowSize.Height > 0 {
		return tea.Batch(appInitCmd, func() tea.Msg { return m.windowSize })
	}
	return appInitCmd
}

func (m *Model) splitCommandIntoCommandAndArgs(command string) (string, []string) {
	command = strings.TrimSpace(command)
	if command == "" {
		return "", nil
	}

	// Find first space to separate command from args
	spaceIndex := strings.Index(command, " ")
	if spaceIndex == -1 {
		return command, nil
	}

	cmd := command[:spaceIndex]
	argsStr := strings.TrimSpace(command[spaceIndex+1:])

	if argsStr == "" {
		return cmd, nil
	}

	// Parse arguments handling quotes
	var args []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(argsStr); i++ {
		char := argsStr[i]

		switch {
		case !inQuotes && (char == '"' || char == '\''):
			// Start of quoted string
			inQuotes = true
			quoteChar = char
		case inQuotes && char == quoteChar:
			// End of quoted string
			inQuotes = false
			quoteChar = 0
		case !inQuotes && char == ' ':
			// Space separator
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(char)
		}
	}

	// Add final argument if any
	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return cmd, args
}
