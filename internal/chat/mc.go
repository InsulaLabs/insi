package chat

import (
	"strings"

	"github.com/InsulaLabs/insi/internal/app"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const (
	gap = "\n"
)

func (x *App) viewModeChat() string {
	content := lipgloss.JoinVertical(
		lipgloss.Top,
		x.viewport.View(),
		gap,
		x.textarea.View(),
	)
	// Ensure the content fits exactly in the terminal height
	return lipgloss.NewStyle().Height(x.height).Render(content)
}

func (x *App) updateModeChat(msg tea.Msg) (app.App, tea.Cmd) {
	var (
		tiCmd tea.Cmd
		vpCmd tea.Cmd
	)

	x.textarea, tiCmd = x.textarea.Update(msg)
	x.viewport, vpCmd = x.viewport.Update(msg)

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		// Handle full-screen layout
		x.height = msg.Height
		x.viewport.Width = msg.Width
		x.textarea.SetWidth(msg.Width)

		// Calculate required heights
		textareaHeight := x.textarea.Height()
		gapHeight := lipgloss.Height(gap)
		minViewportHeight := 3
		totalRequiredHeight := textareaHeight + gapHeight + minViewportHeight

		// If terminal is too small, adjust textarea height dynamically
		if msg.Height < totalRequiredHeight {
			// Reduce textarea height to minimum (1 line) to fit more content
			x.textarea.SetHeight(1)
			textareaHeight = 1
			x.viewport.Height = msg.Height - textareaHeight - gapHeight

			// Ensure viewport has at least 1 line
			if x.viewport.Height < 1 {
				x.viewport.Height = 1
			}
		} else {
			// Normal case: textarea at preferred height (3)
			x.textarea.SetHeight(3)
			x.viewport.Height = msg.Height - textareaHeight - gapHeight
		}

		if len(x.messages) > 0 {
			// Wrap content before setting it.
			x.viewport.SetContent(lipgloss.NewStyle().Width(x.viewport.Width).Render(strings.Join(x.messages, "\n")))
		}
		x.viewport.GotoBottom()
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			return nil, tea.Quit
		case tea.KeyCtrlQ:
			x.userInterrupt()
		case tea.KeyEnter:
			if strings.TrimSpace(x.textarea.Value()) != "" {
				x.messages = append(x.messages, x.senderStyle.Render("You: ")+x.textarea.Value())
				x.viewport.SetContent(lipgloss.NewStyle().Width(x.viewport.Width).Render(strings.Join(x.messages, "\n")))
				x.textarea.Reset()
				x.viewport.GotoBottom()
			}
		}
	}

	return x, tea.Batch(tiCmd, vpCmd)
}
