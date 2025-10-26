package editor

import (
	"strings"

	"github.com/InsulaLabs/insi/internal/app"

	tea "github.com/charmbracelet/bubbletea"
)

func (e *App) viewModeHelp() string {
	b := strings.Builder{}
	b.WriteString("\nEditor Command Reference\n")
	b.WriteString("========================\n\n")
	b.WriteString("  editor <filename>  - Open a file for editing.\n")
	b.WriteString("  editor help        - Show this help information.\n\n")
	b.WriteString("Editor Controls:\n")
	b.WriteString("  Ctrl+S     - Save the current file.\n")
	b.WriteString("  Ctrl+Q     - Quit editor (force quit even with unsaved changes).\n")
	b.WriteString("  Ctrl+C     - Exit editor (prompts if unsaved changes).\n")
	b.WriteString("  ESC        - Exit editor (prompts if unsaved changes).\n\n")
	b.WriteString("Navigation:\n")
	b.WriteString("  Arrow keys - Move cursor.\n")
	b.WriteString("  Page Up/Down - Navigate through file.\n")
	b.WriteString("  Home/End   - Jump to beginning/end of line.\n\n")
	b.WriteString("Press Enter to exit this help screen.\n")
	return b.String()
}

func (e *App) updateModeHelp(msg tea.Msg) (app.App, tea.Cmd) {
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
