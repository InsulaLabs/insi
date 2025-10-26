package app

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/InsulaLabs/insi/internal/db/core"
	"github.com/InsulaLabs/insi/pkg/fwi"
	tea "github.com/charmbracelet/bubbletea"
)

type sessionVFS struct {
	activeDirectory string // inits to "/"
	fs              fwi.FS
}

func getVfsCommandMap(ctx context.Context, extensionControls []core.ExtensionControl) map[string]CLICmdHandler {
	commands := map[string]CLICmdHandler{
		"pwd": func(session *Session, command string, args []string) tea.Cmd {
			return func() tea.Msg {
				return commandOutputMsg{output: session.GetCurrentDirectory(), isErr: false}
			}
		},
		"cd": func(session *Session, command string, args []string) tea.Cmd {
			var targetDir string
			if len(args) == 0 {
				targetDir = "/"
			} else {
				targetDir = args[0]
			}

			err := session.ChangeDirectory(ctx, targetDir)
			if err != nil {
				return func() tea.Msg {
					return commandOutputMsg{output: err.Error(), isErr: true}
				}
			}

			return func() tea.Msg {
				return commandOutputMsg{output: "", isErr: false}
			}
		},
		"ls": func(session *Session, command string, args []string) tea.Cmd {
			targetDir := session.virtualFileSystem.activeDirectory
			if len(args) > 0 {
				targetDir = session.ResolvePath(args[0])
			}

			// Ensure the target directory exists
			if err := ensureDirectoryExists(ctx, session.virtualFileSystem.fs, targetDir); err != nil {
				return func() tea.Msg {
					return commandOutputMsg{output: fmt.Sprintf("Error creating directory: %s", err.Error()), isErr: true}
				}
			}

			entries, err := session.virtualFileSystem.fs.ReadDir(ctx, targetDir)
			if err != nil {
				return func() tea.Msg {
					return commandOutputMsg{output: err.Error(), isErr: true}
				}
			}

			// Format the output similar to Unix ls
			var output strings.Builder
			for _, entry := range entries {
				fileType := "f"
				if entry.IsDir() {
					fileType = "d"
				}
				size := fmt.Sprintf("%d", entry.Size())
				modTime := entry.ModTime().Format("Jan 02 15:04")
				output.WriteString(fmt.Sprintf("%s %8s %s %s\n", fileType, size, modTime, entry.Name()))
			}

			return func() tea.Msg {
				return commandOutputMsg{output: output.String(), isErr: false}
			}
		},
		"cat": func(session *Session, command string, args []string) tea.Cmd {
			if len(args) == 0 {
				return func() tea.Msg {
					return commandOutputMsg{output: "Usage: cat <filename>", isErr: true}
				}
			}

			filePath := session.ResolvePath(args[0])

			file, err := session.virtualFileSystem.fs.Open(ctx, filePath)
			if err != nil {
				return func() tea.Msg {
					return commandOutputMsg{output: fmt.Sprintf("Error opening file: %s", err.Error()), isErr: true}
				}
			}
			defer file.Close()

			content, err := io.ReadAll(file)
			if err != nil {
				return func() tea.Msg {
					return commandOutputMsg{output: fmt.Sprintf("Error reading file: %s", err.Error()), isErr: true}
				}
			}

			return func() tea.Msg {
				return commandOutputMsg{output: string(content), isErr: false}
			}
		},
		"mkdir": func(session *Session, command string, args []string) tea.Cmd {
			if len(args) == 0 {
				return func() tea.Msg {
					return commandOutputMsg{output: "Usage: mkdir <directory>", isErr: true}
				}
			}

			dirPath := session.ResolvePath(args[0])

			err := session.virtualFileSystem.fs.Mkdir(ctx, dirPath)
			if err != nil {
				return func() tea.Msg {
					return commandOutputMsg{output: fmt.Sprintf("Error creating directory: %s", err.Error()), isErr: true}
				}
			}

			return func() tea.Msg {
				return commandOutputMsg{output: "", isErr: false}
			}
		},
		"touch": func(session *Session, command string, args []string) tea.Cmd {
			if len(args) == 0 {
				return func() tea.Msg {
					return commandOutputMsg{output: "Usage: touch <filename>", isErr: true}
				}
			}

			filePath := session.ResolvePath(args[0])

			// Check if file already exists
			_, err := session.virtualFileSystem.fs.Stat(ctx, filePath)
			if err == nil {
				// File exists, just update the modification time by reopening and closing
				file, err := session.virtualFileSystem.fs.Open(ctx, filePath)
				if err != nil {
					return func() tea.Msg {
						return commandOutputMsg{output: fmt.Sprintf("Error accessing file: %s", err.Error()), isErr: true}
					}
				}
				file.Close()
			} else {
				// File doesn't exist, create it
				file, err := session.virtualFileSystem.fs.Create(ctx, filePath)
				if err != nil {
					return func() tea.Msg {
						return commandOutputMsg{output: fmt.Sprintf("Error creating file: %s", err.Error()), isErr: true}
					}
				}
				file.Close()
			}

			return func() tea.Msg {
				return commandOutputMsg{output: "", isErr: false}
			}
		},
		"rm": func(session *Session, command string, args []string) tea.Cmd {
			if len(args) == 0 {
				return func() tea.Msg {
					return commandOutputMsg{output: "Usage: rm [-r] <path>", isErr: true}
				}
			}

			var recursive bool
			var path string

			if len(args) >= 2 && args[0] == "-r" {
				recursive = true
				path = args[1]
			} else {
				path = args[0]
			}

			filePath := session.ResolvePath(path)

			var opts []fwi.RemoveOption
			if recursive {
				opts = append(opts, fwi.WithRecursiveRemove())
			}

			err := session.virtualFileSystem.fs.Remove(ctx, filePath, opts...)
			if err != nil {
				return func() tea.Msg {
					return commandOutputMsg{output: fmt.Sprintf("Error removing: %s", err.Error()), isErr: true}
				}
			}

			return func() tea.Msg {
				return commandOutputMsg{output: "", isErr: false}
			}
		},
	}
	return commands
}
