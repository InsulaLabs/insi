package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/InsulaLabs/insi/internal/db/core"
	"github.com/InsulaLabs/insi/pkg/fwi"
	tea "github.com/charmbracelet/bubbletea"
)

func getAccountHelpText() string {
	return `
User Account Management Commands:

  account help                                  - Show this help
  account info                                  - Show account usage and limits
  account limits                                - Get all limits for account

  account ssh key add <public-key>       - Add SSH public key
  account ssh key list                   - List SSH public keys
  account ssh key delete <public-key>    - Delete SSH public key

  account alias new                      - Create new alias (returns API key)
  account alias list                     - List all aliases
  account alias delete <api-key>         - Delete an alias
`
}

func getAccountCommands(ctx context.Context, session *Session, extensionControls []core.ExtensionControl) map[string]CLICmdHandler {
	commands := map[string]CLICmdHandler{
		"account": buildAccountHandler(ctx, session),
	}
	return commands
}

func buildAccountHandler(ctx context.Context, session *Session) CLICmdHandler {
	return func(s *Session, command string, args []string) tea.Cmd {
		if len(args) == 0 {
			return outputCmd(getAccountHelpText(), false)
		}

		subCmd := args[0]

		switch subCmd {
		case "help":
			return outputCmd(getAccountHelpText(), false)
		case "info":
			return handleAccountInfo(ctx, s)
		case "ssh":
			return handleAccountSSH(ctx, s, args[1:])
		case "alias":
			return handleAccountAlias(ctx, s, args[1:])
		case "limits":
			return handleAccountLimitsGet(ctx, s)
		default:
			return outputCmd(fmt.Sprintf("Error: unknown command '%s'\n"+getAccountHelpText(), subCmd), true)
		}
	}
}

func handleAccountInfo(ctx context.Context, s *Session) tea.Cmd {
	if s.userFWI == nil {
		return outputCmd("Error: User FWI not available", true)
	}

	usageInfo, err := s.userFWI.GetUsageInfo(ctx)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error getting account info: %v", err), true)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Account: %s (UUID: %s)\n\n", s.userFWI.GetName(), s.userFWI.GetUUID()))

	if usageInfo.CurrentUsage != nil {
		sb.WriteString("Current Usage:\n")
		u := usageInfo.CurrentUsage
		if u.BytesOnDisk != nil {
			sb.WriteString(fmt.Sprintf("  Bytes on Disk:     %d\n", *u.BytesOnDisk))
		}
		if u.BytesInMemory != nil {
			sb.WriteString(fmt.Sprintf("  Bytes in Memory:   %d\n", *u.BytesInMemory))
		}
		if u.EventsEmitted != nil {
			sb.WriteString(fmt.Sprintf("  Events Emitted:    %d\n", *u.EventsEmitted))
		}
		if u.Subscribers != nil {
			sb.WriteString(fmt.Sprintf("  Subscribers:       %d\n", *u.Subscribers))
		}
		if u.RPSDataLimit != nil {
			sb.WriteString(fmt.Sprintf("  RPS Data Limit:    %d\n", *u.RPSDataLimit))
		}
		if u.RPSEventLimit != nil {
			sb.WriteString(fmt.Sprintf("  RPS Event Limit:   %d\n", *u.RPSEventLimit))
		}
		sb.WriteString("\n")
	}

	if usageInfo.MaxLimits != nil {
		sb.WriteString("Max Limits:\n")
		l := usageInfo.MaxLimits
		if l.BytesOnDisk != nil {
			sb.WriteString(fmt.Sprintf("  Bytes on Disk:     %d\n", *l.BytesOnDisk))
		}
		if l.BytesInMemory != nil {
			sb.WriteString(fmt.Sprintf("  Bytes in Memory:   %d\n", *l.BytesInMemory))
		}
		if l.EventsEmitted != nil {
			sb.WriteString(fmt.Sprintf("  Events Emitted:    %d\n", *l.EventsEmitted))
		}
		if l.Subscribers != nil {
			sb.WriteString(fmt.Sprintf("  Subscribers:       %d\n", *l.Subscribers))
		}
		if l.RPSDataLimit != nil {
			sb.WriteString(fmt.Sprintf("  RPS Data Limit:    %d\n", *l.RPSDataLimit))
		}
		if l.RPSEventLimit != nil {
			sb.WriteString(fmt.Sprintf("  RPS Event Limit:   %d\n", *l.RPSEventLimit))
		}
	}

	return outputCmd(sb.String(), false)
}

func handleAccountSSH(ctx context.Context, s *Session, args []string) tea.Cmd {
	if s.userFWI == nil {
		return outputCmd("Error: User FWI not available", true)
	}

	if len(args) < 2 {
		return outputCmd("Error: ssh subcommand required\nUsage: account ssh key <add|list|delete> ...", true)
	}

	if args[0] != "key" {
		return outputCmd("Error: 'key' keyword required after 'ssh'", true)
	}

	subCmd := args[1]

	switch subCmd {
	case "add":
		return handleAccountSSHKeyAdd(ctx, s.userFWI, args[2:])
	case "list":
		return handleAccountSSHKeyList(ctx, s.userFWI)
	case "delete":
		return handleAccountSSHKeyDelete(ctx, s.userFWI, args[2:])
	default:
		return outputCmd(fmt.Sprintf("Error: unknown ssh key command '%s'", subCmd), true)
	}
}

func handleAccountSSHKeyAdd(ctx context.Context, entity fwi.Entity, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: public key required\nUsage: account ssh key add <public-key>", true)
	}

	publicKey := strings.Join(args, " ")

	if err := entity.AddPublicKey(ctx, publicKey); err != nil {
		return outputCmd(fmt.Sprintf("Error adding public key: %v", err), true)
	}

	return outputCmd("Public key added successfully.", false)
}

func handleAccountSSHKeyList(ctx context.Context, entity fwi.Entity) tea.Cmd {
	keys, err := entity.ListPublicKeys(ctx)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error listing public keys: %v", err), true)
	}

	if len(keys) == 0 {
		return outputCmd("No public keys found.", false)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Found %d public key(s):\n\n", len(keys)))
	for i, key := range keys {
		sb.WriteString(fmt.Sprintf("%d. %s\n", i+1, key))
	}

	return outputCmd(sb.String(), false)
}

func handleAccountSSHKeyDelete(ctx context.Context, entity fwi.Entity, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: public key required\nUsage: account ssh key delete <public-key>", true)
	}

	publicKey := strings.Join(args, " ")

	if err := entity.RemovePublicKey(ctx, publicKey); err != nil {
		return outputCmd(fmt.Sprintf("Error deleting public key: %v", err), true)
	}

	return outputCmd("Public key deleted successfully.", false)
}

func handleAccountAlias(ctx context.Context, s *Session, args []string) tea.Cmd {
	if s.userFWI == nil {
		return outputCmd("Error: User FWI not available", true)
	}

	if len(args) == 0 {
		return outputCmd("Error: alias subcommand required\nUsage: account alias <new|list|delete> ...", true)
	}

	subCmd := args[0]

	aliasManager := s.userFWI.GetAliasManager()

	switch subCmd {
	case "new":
		return handleAccountAliasNew(ctx, aliasManager)
	case "list":
		return handleAccountAliasList(ctx, aliasManager)
	case "delete":
		return handleAccountAliasDelete(ctx, aliasManager, args[1:])
	default:
		return outputCmd(fmt.Sprintf("Error: unknown alias command '%s'", subCmd), true)
	}
}

func handleAccountAliasNew(ctx context.Context, aliasManager fwi.Aliases) tea.Cmd {
	alias, err := aliasManager.MakeAlias(ctx)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error creating alias: %v", err), true)
	}

	return outputCmd(fmt.Sprintf("Alias created successfully:\n  %s\n", alias), false)
}

func handleAccountAliasList(ctx context.Context, aliasManager fwi.Aliases) tea.Cmd {
	aliases, err := aliasManager.ListAliases(ctx)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error listing aliases: %v", err), true)
	}

	if len(aliases) == 0 {
		return outputCmd("No aliases found.", false)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Found %d alias(es):\n\n", len(aliases)))
	for i, alias := range aliases {
		sb.WriteString(fmt.Sprintf("%d. %s\n", i+1, alias))
	}

	return outputCmd(sb.String(), false)
}

func handleAccountAliasDelete(ctx context.Context, aliasManager fwi.Aliases, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: alias required\nUsage: account alias delete <api-key>", true)
	}

	alias := args[0]

	if err := aliasManager.DeleteAlias(ctx, alias); err != nil {
		return outputCmd(fmt.Sprintf("Error deleting alias: %v", err), true)
	}

	return outputCmd("Alias deleted successfully.", false)
}

func handleAccountLimitsGet(ctx context.Context, s *Session) tea.Cmd {
	usageInfo, err := s.userFWI.GetUsageInfo(ctx)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error getting limits: %v", err), true)
	}

	var sb strings.Builder
	sb.WriteString("Account Limits:\n\n")

	if usageInfo.MaxLimits != nil {
		l := usageInfo.MaxLimits
		if l.BytesOnDisk != nil {
			sb.WriteString(fmt.Sprintf("  bytes_on_disk:     %d\n", *l.BytesOnDisk))
		}
		if l.BytesInMemory != nil {
			sb.WriteString(fmt.Sprintf("  bytes_in_memory:   %d\n", *l.BytesInMemory))
		}
		if l.EventsEmitted != nil {
			sb.WriteString(fmt.Sprintf("  events_emitted:    %d\n", *l.EventsEmitted))
		}
		if l.Subscribers != nil {
			sb.WriteString(fmt.Sprintf("  subscribers:       %d\n", *l.Subscribers))
		}
		if l.RPSDataLimit != nil {
			sb.WriteString(fmt.Sprintf("  rps_data_limit:    %d\n", *l.RPSDataLimit))
		}
		if l.RPSEventLimit != nil {
			sb.WriteString(fmt.Sprintf("  rps_event_limit:   %d\n", *l.RPSEventLimit))
		}
	}

	return outputCmd(sb.String(), false)
}
