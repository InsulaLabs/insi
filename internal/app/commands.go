package app

import (
	"github.com/InsulaLabs/insi/internal/db/core"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/log"
)

func getCommandMap(extensionControls []core.ExtensionControl) map[string]CLICmdHandler {
	commands := map[string]CLICmdHandler{
		"exit": func(session *Session, command string, args []string) tea.Cmd {
			return tea.Quit
		},
	}

	for _, extension := range extensionControls {
		commands[extension.CommandName()] = func(session *Session, command string, args []string) tea.Cmd {
			response, err := extension.HandleCommand(command, args)
			if err != nil {
				log.Error("Failed to handle command", "error", err)
				return tea.Println(err.Error())
			}
			return tea.Println(response)
		}
	}
	return commands
}
