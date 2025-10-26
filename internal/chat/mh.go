package chat

import (
	"strings"

	"github.com/InsulaLabs/insi/internal/app"

	tea "github.com/charmbracelet/bubbletea"
)

func (x *App) viewModeHelp() string {
	b := strings.Builder{}
	b.WriteString("\nChat Command Reference\n")
	b.WriteString("======================\n\n")
	b.WriteString("  chat            - Open a new chat session.\n")
	b.WriteString("  chat list       - List all available chats. Select one to open.\n")
	b.WriteString("  chat join <ID>  - Join or load the chat with the given <ID>.\n")
	b.WriteString("  chat help       - Show this help information.\n\n")
	b.WriteString("Usage Tips:\n")
	b.WriteString("  • Press Enter to send a message in chat mode.\n")
	b.WriteString("  • Ctrl+C or Enter exits this help screen.\n")
	return b.String()
}

func (x *App) updateModeHelp(msg tea.Msg) (app.App, tea.Cmd) {
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
