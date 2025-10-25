package repl

import (
	"strings"
	"time"

	"github.com/InsulaLabs/insi/internal/app"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/log"
)

type tickMsg time.Time

type Model struct {
	session    *app.Session
	buffer     string
	cursor     int
	quitting   bool
	cursorOn   bool
	currentApp app.App
	windowSize tea.WindowSizeMsg

	applications app.AppMap
}

type ReplConfig struct {
	SessionConfig app.SessionConfig
}

func New(config ReplConfig, applications app.AppMap) Model {
	session := app.NewSession(config.SessionConfig)
	return Model{
		session:      session,
		buffer:       "",
		cursor:       0,
		cursorOn:     true,
		applications: applications,
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
		case tea.KeyCtrlC, tea.KeyCtrlD:
			m.quitting = true
			return m, tea.Quit
		case tea.KeyEnter:
			command := strings.TrimSpace(m.buffer)
			m.buffer = ""
			m.cursor = 0
			if command != "" {
				log.Info("Command received", "command", command)

				m.session.AddToHistory(command)

				cmd, args := m.splitCommandIntoCommandAndArgs(command)
				if app, ok := m.applications[cmd]; ok {
					return m, m.LaunchApp(app(), args)
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
		default:
			if len(msg.String()) == 1 {
				char := msg.String()
				m.buffer = m.buffer[:m.cursor] + char + m.buffer[m.cursor:]
				m.cursor++
			}
			return m, nil
		}
	case tickMsg:
		m.cursorOn = !m.cursorOn
		return m, tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
			return tickMsg(t)
		})
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
	b.WriteString("Type 'exit' or press Ctrl+C/Ctrl+D to quit.\n")
	b.WriteString("Type 'tester' to launch the calculator app.\n\n")

	for _, cmd := range m.session.GetHistory() {
		b.WriteString(m.session.GetPrompt())
		b.WriteString(cmd)
		b.WriteString("\n")
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

func (m *Model) LaunchApp(app app.App, args []string) tea.Cmd {
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
