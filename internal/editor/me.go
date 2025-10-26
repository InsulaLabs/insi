package editor

import (
	"fmt"

	"github.com/InsulaLabs/insi/internal/app"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

func (e *App) viewModeEdit() string {
	status := e.renderStatusLine()
	editorView := e.textarea.View()
	
	return lipgloss.JoinVertical(
		lipgloss.Top,
		editorView,
		status,
	)
}

func (e *App) updateModeEdit(msg tea.Msg) (app.App, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		e.height = msg.Height
		e.width = msg.Width
		e.textarea.SetWidth(msg.Width)
		e.textarea.SetHeight(msg.Height - 1)
		
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			if !e.isDirty() {
				return nil, tea.Quit
			}
		case tea.KeyEsc:
			if !e.isDirty() {
				return nil, tea.Quit
			}
		case tea.KeyCtrlS:
			if err := e.saveFile(); err != nil {
				e.viewport.SetContent(fmt.Sprintf("Error saving: %v", err))
			} else {
				e.viewport.SetContent("File saved")
			}
		case tea.KeyCtrlQ:
			return nil, tea.Quit
		}
	}

	e.textarea, cmd = e.textarea.Update(msg)
	return e, cmd
}

func (e *App) renderStatusLine() string {
	dirtyIndicator := ""
	if e.isDirty() {
		dirtyIndicator = " [modified]"
	}
	
	status := fmt.Sprintf(" %s%s | Ctrl+S: Save | Ctrl+Q: Quit | ESC: Exit", e.filePath, dirtyIndicator)
	
	return e.statusLineStyle.Width(e.width).Render(status)
}
