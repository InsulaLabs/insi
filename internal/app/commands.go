package app

import (
	"context"

	"github.com/InsulaLabs/insi/internal/db/core"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/log"
)

/*
Note:
These command extensions are the extensions for the insi core that are loaded in the local
nodes runtime. Its possible that these are running on other nodes in the insi cluster, but
its not mandated that they must be. For this reason, nerv is only in control of the apex
extensions. This is ultimately by design. If we want to extend later to make a "cluster wide"
extension manager from the apex node we would need to come up with a way to sync across the
available nodes.

This is out of scope for the actual ssh application (model+session) but the note is left here
because if anyone is attempting to start a process over ssh and wonders why they can't do so
on any given node they will ultimately trace their investigations to this point.

If cross-cluster extension controll is wanted, make an extension for it and load it here as a
"middle man" extension that handles the cross-cluster stuff under the hood and then it will
be immediatly available here when registered with the runtime in insid or whatever binary
is hosting the insi Runtime.
*/
func getCommandMap(ctx context.Context, session *Session, extensionControls []core.ExtensionControl) map[string]CLICmdHandler {
	commands := map[string]CLICmdHandler{
		"exit": func(session *Session, command string, args []string) tea.Cmd {
			return tea.Quit
		},
		"help": func(session *Session, command string, args []string) tea.Cmd {
			helpText := session.BuildHelpText()
			return func() tea.Msg {
				return commandOutputMsg{output: helpText, isErr: false}
			}
		},
	}

	commands = mergeCommandMaps(commands, getVfsCommandMap(ctx, extensionControls))

	for _, extension := range extensionControls {
		commands[extension.CommandName()] = func(session *Session, command string, args []string) tea.Cmd {
			response, err := extension.HandleCommand(command, args)
			if err != nil {
				log.Error("Failed to handle command", "error", err)
				return func() tea.Msg {
					return commandOutputMsg{output: err.Error(), isErr: true}
				}
			}
			return func() tea.Msg {
				return commandOutputMsg{output: response, isErr: false}
			}
		}
	}
	return commands
}

func mergeCommandMaps(maps ...map[string]CLICmdHandler) map[string]CLICmdHandler {
	result := make(map[string]CLICmdHandler)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}
