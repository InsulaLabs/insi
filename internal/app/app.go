package app

import tea "github.com/charmbracelet/bubbletea"

type AppConstructor func() App

type AppMap map[string]AppConstructor

type App interface {
	Init(session *Session, args []string) tea.Cmd
	Update(msg tea.Msg) (App, tea.Cmd)
	View() string
	GetHelpText() string
}
