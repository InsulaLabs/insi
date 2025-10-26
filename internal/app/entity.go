package app

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/InsulaLabs/insi/internal/db/core"
	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/models"
	tea "github.com/charmbracelet/bubbletea"
)

func getRestrictedCommands(ctx context.Context, session *Session, extensionControls []core.ExtensionControl) map[string]CLICmdHandler {
	// There will be more than entity later
	return mergeCommandMaps(getEntityCommands(ctx, session, extensionControls))
}

func getEntityHelpText() string {
	return `
Entity Management Commands:

  entity help                                  - Show this help
  entity list                                  - List all entities
  entity new <name>                            - Create a new entity (with default limits)
  entity delete <name>                         - Delete an entity
  
  entity <name> info                           - Show entity usage and limits
  entity <name> limit get                      - Get all limits for entity
  entity <name> limit set <limit-name> <value> - Set a specific limit
  
  entity <name> ssh key add <public-key>       - Add SSH public key
  entity <name> ssh key list                   - List SSH public keys
  entity <name> ssh key delete <public-key>    - Delete SSH public key
  
  entity <name> alias new                      - Create new alias (returns API key)
  entity <name> alias list                     - List all aliases
  entity <name> alias delete <api-key>         - Delete an alias

Available limit names:
  - bytes_on_disk       (int64)
  - bytes_in_memory     (int64)
  - events_emitted      (int64)
  - subscribers         (int64)
  - rps_data_limit      (int64)
  - rps_event_limit     (int64)
`
}

func getEntityCommands(ctx context.Context, session *Session, extensionControls []core.ExtensionControl) map[string]CLICmdHandler {
	entityMap := map[string]CLICmdHandler{
		"entity": buildEntityHandler(ctx, session),
	}
	return entityMap
}

func buildEntityHandler(ctx context.Context, session *Session) CLICmdHandler {
	return func(s *Session, command string, args []string) tea.Cmd {
		if len(args) == 0 {
			return outputCmd(getEntityHelpText(), false)
		}

		subCmd := args[0]

		switch subCmd {
		case "help":
			return outputCmd(getEntityHelpText(), false)
		case "list":
			return handleEntityList(ctx, s)
		case "new":
			return handleEntityNew(ctx, s, args[1:])
		case "delete":
			return handleEntityDelete(ctx, s, args[1:])
		default:
			return handleEntityNamedCommand(ctx, s, args)
		}
	}
}

func outputCmd(output string, isErr bool) tea.Cmd {
	return func() tea.Msg {
		return commandOutputMsg{output: output, isErr: isErr}
	}
}

func handleEntityList(ctx context.Context, s *Session) tea.Cmd {
	if s.fwiManager == nil {
		return outputCmd("Error: FWI manager not available", true)
	}

	entities, err := s.fwiManager.GetAllEntities(ctx)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error listing entities: %v", err), true)
	}

	if len(entities) == 0 {
		return outputCmd("No entities found.", false)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Found %d entities:\n\n", len(entities)))
	for _, entity := range entities {
		sb.WriteString(fmt.Sprintf("  - %s (UUID: %s)\n", entity.GetName(), entity.GetUUID()))
	}

	return outputCmd(sb.String(), false)
}

func handleEntityNew(ctx context.Context, s *Session, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: entity name required\nUsage: entity new <name>", true)
	}

	if s.fwiManager == nil {
		return outputCmd("Error: FWI manager not available", true)
	}

	name := args[0]

	defaultLimits := models.Limits{
		BytesOnDisk:   ptrInt64(1024 * 1024 * 1024),
		BytesInMemory: ptrInt64(1024 * 1024 * 1024),
		EventsEmitted: ptrInt64(1000),
		Subscribers:   ptrInt64(100),
		RPSDataLimit:  ptrInt64(models.DefaultRPSDataLimit),
		RPSEventLimit: ptrInt64(models.DefaultRPSEventLimit),
	}

	entity, err := s.fwiManager.CreateEntity(ctx, name, defaultLimits)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error creating entity: %v", err), true)
	}

	return outputCmd(fmt.Sprintf("Entity created successfully:\n  Name: %s\n  UUID: %s\n  API Key: %s\n",
		entity.GetName(), entity.GetUUID(), entity.GetKey()), false)
}

func handleEntityDelete(ctx context.Context, s *Session, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: entity name required\nUsage: entity delete <name>", true)
	}

	if s.fwiManager == nil {
		return outputCmd("Error: FWI manager not available", true)
	}

	name := args[0]

	if err := s.fwiManager.DeleteEntity(ctx, name); err != nil {
		return outputCmd(fmt.Sprintf("Error deleting entity: %v", err), true)
	}

	return outputCmd(fmt.Sprintf("Entity '%s' deleted successfully.", name), false)
}

func handleEntityNamedCommand(ctx context.Context, s *Session, args []string) tea.Cmd {
	if len(args) < 2 {
		return outputCmd("Error: insufficient arguments\n"+getEntityHelpText(), true)
	}

	if s.fwiManager == nil {
		return outputCmd("Error: FWI manager not available", true)
	}

	entityName := args[0]
	subCmd := args[1]

	entity, err := s.fwiManager.GetEntity(ctx, entityName)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error: entity '%s' not found: %v", entityName, err), true)
	}

	switch subCmd {
	case "info":
		return handleEntityInfo(ctx, entity)
	case "limit":
		return handleEntityLimit(ctx, s, entity.GetName(), args[2:])
	case "ssh":
		return handleEntitySSH(ctx, entity, args[2:])
	case "alias":
		return handleEntityAlias(ctx, entity.GetAliasManager(), args[2:])
	default:
		return outputCmd(fmt.Sprintf("Error: unknown command '%s'\n"+getEntityHelpText(), subCmd), true)
	}
}

func handleEntityInfo(ctx context.Context, entity fwi.Entity) tea.Cmd {
	usageInfo, err := entity.GetUsageInfo(ctx)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error getting entity info: %v", err), true)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Entity: %s (UUID: %s)\n\n", entity.GetName(), entity.GetUUID()))

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

func handleEntityLimit(ctx context.Context, s *Session, entityName string, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: limit subcommand required\nUsage: entity <name> limit <get|set> ...", true)
	}

	subCmd := args[0]

	switch subCmd {
	case "get":
		return handleEntityLimitGet(ctx, s, entityName)
	case "set":
		return handleEntityLimitSet(ctx, s, entityName, args[1:])
	default:
		return outputCmd(fmt.Sprintf("Error: unknown limit command '%s'", subCmd), true)
	}
}

func handleEntityLimitGet(ctx context.Context, s *Session, entityName string) tea.Cmd {
	entity, err := s.fwiManager.GetEntity(ctx, entityName)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error: %v", err), true)
	}

	usageInfo, err := entity.GetUsageInfo(ctx)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error getting limits: %v", err), true)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Limits for entity '%s':\n\n", entityName))

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

func handleEntityLimitSet(ctx context.Context, s *Session, entityName string, args []string) tea.Cmd {
	if len(args) < 2 {
		return outputCmd("Error: limit name and value required\nUsage: entity <name> limit set <limit-name> <value>", true)
	}

	limitName := args[0]
	limitValueStr := args[1]

	limitValue, err := strconv.ParseInt(limitValueStr, 10, 64)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error: invalid limit value '%s': %v", limitValueStr, err), true)
	}

	newLimits := models.Limits{}

	switch limitName {
	case "bytes_on_disk":
		newLimits.BytesOnDisk = &limitValue
	case "bytes_in_memory":
		newLimits.BytesInMemory = &limitValue
	case "events_emitted":
		newLimits.EventsEmitted = &limitValue
	case "subscribers":
		newLimits.Subscribers = &limitValue
	case "rps_data_limit":
		newLimits.RPSDataLimit = &limitValue
	case "rps_event_limit":
		newLimits.RPSEventLimit = &limitValue
	default:
		return outputCmd(fmt.Sprintf("Error: unknown limit name '%s'", limitName), true)
	}

	if err := s.fwiManager.UpdateEntityLimits(ctx, entityName, newLimits); err != nil {
		return outputCmd(fmt.Sprintf("Error updating limit: %v", err), true)
	}

	return outputCmd(fmt.Sprintf("Successfully set %s to %d for entity '%s'", limitName, limitValue, entityName), false)
}

func handleEntitySSH(ctx context.Context, entity fwi.Entity, args []string) tea.Cmd {
	if len(args) < 2 {
		return outputCmd("Error: ssh subcommand required\nUsage: entity <name> ssh key <add|list|delete> ...", true)
	}

	if args[0] != "key" {
		return outputCmd("Error: 'key' keyword required after 'ssh'", true)
	}

	subCmd := args[1]

	switch subCmd {
	case "add":
		return handleEntitySSHKeyAdd(ctx, entity, args[2:])
	case "list":
		return handleEntitySSHKeyList(ctx, entity)
	case "delete":
		return handleEntitySSHKeyDelete(ctx, entity, args[2:])
	default:
		return outputCmd(fmt.Sprintf("Error: unknown ssh key command '%s'", subCmd), true)
	}
}

func handleEntitySSHKeyAdd(ctx context.Context, entity fwi.Entity, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: public key required\nUsage: entity <name> ssh key add <public-key>", true)
	}

	publicKey := strings.Join(args, " ")

	if err := entity.AddPublicKey(ctx, publicKey); err != nil {
		return outputCmd(fmt.Sprintf("Error adding public key: %v", err), true)
	}

	return outputCmd("Public key added successfully.", false)
}

func handleEntitySSHKeyList(ctx context.Context, entity fwi.Entity) tea.Cmd {
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

func handleEntitySSHKeyDelete(ctx context.Context, entity fwi.Entity, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: public key required\nUsage: entity <name> ssh key delete <public-key>", true)
	}

	publicKey := strings.Join(args, " ")

	if err := entity.RemovePublicKey(ctx, publicKey); err != nil {
		return outputCmd(fmt.Sprintf("Error deleting public key: %v", err), true)
	}

	return outputCmd("Public key deleted successfully.", false)
}

func handleEntityAlias(ctx context.Context, aliasManager fwi.Aliases, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: alias subcommand required\nUsage: entity <name> alias <new|list|delete> ...", true)
	}

	subCmd := args[0]

	switch subCmd {
	case "new":
		return handleEntityAliasNew(ctx, aliasManager)
	case "list":
		return handleEntityAliasList(ctx, aliasManager)
	case "delete":
		return handleEntityAliasDelete(ctx, aliasManager, args[1:])
	default:
		return outputCmd(fmt.Sprintf("Error: unknown alias command '%s'", subCmd), true)
	}
}

func handleEntityAliasNew(ctx context.Context, aliasManager fwi.Aliases) tea.Cmd {
	alias, err := aliasManager.MakeAlias(ctx)
	if err != nil {
		return outputCmd(fmt.Sprintf("Error creating alias: %v", err), true)
	}

	return outputCmd(fmt.Sprintf("Alias created successfully:\n  %s\n", alias), false)
}

func handleEntityAliasList(ctx context.Context, aliasManager fwi.Aliases) tea.Cmd {
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

func handleEntityAliasDelete(ctx context.Context, aliasManager fwi.Aliases, args []string) tea.Cmd {
	if len(args) == 0 {
		return outputCmd("Error: alias required\nUsage: entity <name> alias delete <api-key>", true)
	}

	alias := args[0]

	if err := aliasManager.DeleteAlias(ctx, alias); err != nil {
		return outputCmd(fmt.Sprintf("Error deleting alias: %v", err), true)
	}

	return outputCmd("Alias deleted successfully.", false)
}

func ptrInt64(v int64) *int64 {
	return &v
}
