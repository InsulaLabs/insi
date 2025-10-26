package app

import (
	"context"
	"fmt"

	"github.com/InsulaLabs/insi/internal/db/core"
	tea "github.com/charmbracelet/bubbletea"
)

func getVfsCommandMap(ctx context.Context, extensionControls []core.ExtensionControl) map[string]CLICmdHandler {
	commands := map[string]CLICmdHandler{
		"ls": func(session *Session, command string, args []string) tea.Cmd {
			fs := session.userFWI.GetFS()
			entries, err := fs.ReadDir(ctx, "/")
			if err != nil {
				return func() tea.Msg {
					return commandOutputMsg{output: err.Error(), isErr: true}
				}
			}
			return func() tea.Msg {
				return commandOutputMsg{output: fmt.Sprintf("%v", entries), isErr: false}
			}
		},
	}
	return commands
}
