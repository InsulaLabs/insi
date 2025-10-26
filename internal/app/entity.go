package app

import (
	"context"

	"github.com/InsulaLabs/insi/internal/db/core"
)

func getRestrictedCommands(ctx context.Context, session *Session, extensionControls []core.ExtensionControl) map[string]CLICmdHandler {
	// There will be more than entity later
	return mergeCommandMaps(getEntityCommands(ctx, session, extensionControls))
}

func getEntityHelpText() string {
	return "\nUse 'entity help' to see the available commands."
}

func getEntityCommands(ctx context.Context, session *Session, extensionControls []core.ExtensionControl) map[string]CLICmdHandler {

	/*

		Access commands are just a title for the entity c

			[These are admin only]
				entity
					entity help (show this help)
					entity new  <name (unique)> (defaulkt limits given)

					entity <name> limit set <limit-name> <value>
					entity <name> limit get (gets all)

					entity <name> ssh key add <public key>
					entity <name> ssh key list
					entity <name> ssh key delete <public key>

					entity <name> alias new (returns the alias [an api key])
					entity <name> alias list
					entity <name> alias delete <api key>



	*/
	entityMap := map[string]CLICmdHandler{}
	return entityMap
}
