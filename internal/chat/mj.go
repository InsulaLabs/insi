package chat

import (
	"fmt"

	"github.com/InsulaLabs/insi/internal/app"

	tea "github.com/charmbracelet/bubbletea"
)

func (x *App) viewModeJoin() string {
	return fmt.Sprintf("Joining chat <%s> : NOT YET IMPLEMENTED", x.launchArgs[0])
}

func (x *App) updateModeJoin(msg tea.Msg) (app.App, tea.Cmd) {
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
