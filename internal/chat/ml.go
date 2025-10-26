package chat

import (
	"github.com/InsulaLabs/insi/internal/app"

	tea "github.com/charmbracelet/bubbletea"
)

func (x *App) viewModeList() string {
	return "NOT YET IMPLEMENTED"
}

func (x *App) updateModeList(msg tea.Msg) (app.App, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			return nil, tea.Quit
		case tea.KeyEnter:
			return nil, tea.Quit
		}
	}
	return x, nil
}
