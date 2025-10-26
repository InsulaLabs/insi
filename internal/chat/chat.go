package chat

import (
	"fmt"
	"strings"

	"github.com/InsulaLabs/insi/internal/app"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type ChatMode string

const (
	ChatModeChat  ChatMode = "chat"
	ChatModeList  ChatMode = "list"
	ChatModeJoin  ChatMode = "join"
	ChatModeHelp  ChatMode = "help"
	ChatModeError ChatMode = "error"
)

type App struct {
	viewport    viewport.Model
	messages    []string
	textarea    textarea.Model
	senderStyle lipgloss.Style
	session     *app.Session
	height      int

	launchArgs []string
	mode       ChatMode
	chatError  string // set when invalid args or something dastardly happens
}

func AppEntry() (string, app.AppConstructor) {
	return "chat", new
}

func new() app.App {
	ta := textarea.New()
	ta.Placeholder = "Send a message..."
	ta.Focus()

	ta.Prompt = "┃ "
	ta.CharLimit = 280

	// Start with minimal defaults - proper sizing will happen on WindowSizeMsg
	ta.SetWidth(80)
	ta.SetHeight(3)

	// Remove cursor line styling
	ta.FocusedStyle.CursorLine = lipgloss.NewStyle()

	ta.ShowLineNumbers = false

	// Start with minimal defaults - proper sizing will happen on WindowSizeMsg
	vp := viewport.New(80, 5)
	vp.SetContent(`Welcome to the chat room!
Type a message and press Enter to send.`)

	ta.KeyMap.InsertNewline.SetEnabled(false)

	return &App{
		textarea:    ta,
		messages:    []string{},
		viewport:    vp,
		senderStyle: lipgloss.NewStyle().Foreground(lipgloss.Color("5")),
	}
}

// Init initializes the chat app
func (c *App) Init(session *app.Session, args []string) tea.Cmd {

	c.session = session
	c.launchArgs = args

	if len(args) == 0 {
		c.mode = ChatModeChat
	} else {
		switch strings.ToLower(args[0]) {
		case "list":
			c.mode = ChatModeList
		case "join":
			c.mode = ChatModeJoin
			if len(args) != 2 {
				c.mode = ChatModeError
				c.chatError = "Invalid arguments for join command. Usage: chat join <ID> or chat help"
			}
			c.launchArgs = args[1:]
		case "help":
			c.mode = ChatModeHelp
		case "chat":
			c.mode = ChatModeChat
		default:
			c.mode = ChatModeError
			c.chatError = "Invalid command. Usage: chat list, chat join <ID>, chat help or chat"
		}
	}
	return textarea.Blink
}

// Update handles updates for the chat app
func (c *App) Update(msg tea.Msg) (app.App, tea.Cmd) {
	switch c.mode {
	case ChatModeChat:
		return c.updateModeChat(msg)
	case ChatModeList:
		return c.updateModeList(msg)
	case ChatModeJoin:
		return c.updateModeJoin(msg)
	case ChatModeHelp:
		return c.updateModeHelp(msg)
	case ChatModeError:
		return c.updateModeError(msg)
	}
	return c, nil
}

// View renders the chat app
func (c *App) View() string {
	switch c.mode {
	case ChatModeChat:
		return c.viewModeChat()
	case ChatModeList:
		return c.viewModeList()
	case ChatModeJoin:
		return c.viewModeJoin()
	case ChatModeHelp:
		return c.viewModeHelp()
	case ChatModeError:
		return c.viewModeError()
	}
	return ""
}

func (x *App) userInterrupt() {
	fmt.Println("User interrupted") // this way we can cancel completions easily
}

func (x *App) GetHelpText() string {
	return x.viewModeHelp()
}
