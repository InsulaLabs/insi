package editor

import (
	"github.com/InsulaLabs/insi/internal/app"

	tea "github.com/charmbracelet/bubbletea"
)

func (e *App) viewModeError() string {
	return e.editorError
}

func (e *App) updateModeError(msg tea.Msg) (app.App, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			return nil, tea.Quit
		case tea.KeyEnter:
			return nil, tea.Quit
		}
	}
	return e, nil
}
