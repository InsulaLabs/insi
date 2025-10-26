package editor

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/InsulaLabs/insi/internal/app"
	"github.com/InsulaLabs/insi/pkg/fwi"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type EditorMode string

const (
	EditorModeEdit  EditorMode = "edit"
	EditorModeHelp  EditorMode = "help"
	EditorModeError EditorMode = "error"
)

type App struct {
	viewport viewport.Model
	textarea textarea.Model
	session  *app.Session
	ctx      context.Context
	height   int
	width    int

	filePath        string
	fileContent     string
	originalContent string
	fs              fwi.FS

	mode        EditorMode
	editorError string

	cursorStyle     lipgloss.Style
	cursorLineStyle lipgloss.Style
	statusLineStyle lipgloss.Style
}

var appFS fwi.FS

func AppEntry() (string, app.AppConstructor) {
	return "editor", new
}

func SetFS(fs fwi.FS) {
	appFS = fs
}

func new() app.App {
	ta := textarea.New()
	ta.Placeholder = "Loading..."
	ta.Focus()

	ta.Prompt = ""
	ta.ShowLineNumbers = true

	ta.SetWidth(80)
	ta.SetHeight(20)

	vp := viewport.New(80, 5)
	vp.SetContent("Editor ready")

	ta.KeyMap.InsertNewline.SetEnabled(true)

	return &App{
		textarea:        ta,
		viewport:        vp,
		cursorStyle:     lipgloss.NewStyle().Foreground(lipgloss.Color("212")),
		cursorLineStyle: lipgloss.NewStyle().Background(lipgloss.Color("57")).Foreground(lipgloss.Color("230")),
		statusLineStyle: lipgloss.NewStyle().Background(lipgloss.Color("238")).Foreground(lipgloss.Color("230")),
		fs:              appFS,
	}
}

func (e *App) Init(session *app.Session, args []string) tea.Cmd {
	e.session = session
	e.ctx = session.GetSessionRuntimeCtx()

	if e.fs == nil {
		e.mode = EditorModeError
		e.editorError = "No filesystem available"
		return nil
	}

	if len(args) == 0 {
		e.mode = EditorModeError
		e.editorError = "Usage: editor <filename>"
		return nil
	}

	if strings.ToLower(args[0]) == "help" {
		e.mode = EditorModeHelp
		return nil
	}

	e.filePath = e.session.ResolvePath(args[0])
	e.mode = EditorModeEdit

	if err := e.loadFile(); err != nil {
		e.mode = EditorModeError
		e.editorError = fmt.Sprintf("Failed to load file '%s': %v", e.filePath, err)
		return nil
	}

	return textarea.Blink
}

func (e *App) Update(msg tea.Msg) (app.App, tea.Cmd) {
	switch e.mode {
	case EditorModeEdit:
		return e.updateModeEdit(msg)
	case EditorModeHelp:
		return e.updateModeHelp(msg)
	case EditorModeError:
		return e.updateModeError(msg)
	}
	return e, nil
}

func (e *App) View() string {
	switch e.mode {
	case EditorModeEdit:
		return e.viewModeEdit()
	case EditorModeHelp:
		return e.viewModeHelp()
	case EditorModeError:
		return e.viewModeError()
	}
	return ""
}

func (e *App) GetHelpText() string {
	return e.viewModeHelp()
}

func (e *App) GetDescriptionText() string {
	return "Edit files (use 'editor help' for details)"
}

func (e *App) loadFile() error {
	file, err := e.fs.Open(e.ctx, e.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var buf strings.Builder
	_, err = io.Copy(&buf, file)
	if err != nil {
		return err
	}

	e.fileContent = buf.String()
	e.originalContent = e.fileContent
	e.textarea.SetValue(e.fileContent)
	e.textarea.Placeholder = ""

	return nil
}

func (e *App) saveFile() error {
	file, err := e.fs.Create(e.ctx, e.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	content := e.textarea.Value()
	_, err = io.WriteString(file, content)
	if err != nil {
		return err
	}

	e.fileContent = content
	e.originalContent = content
	return nil
}

func (e *App) isDirty() bool {
	return e.textarea.Value() != e.originalContent
}
