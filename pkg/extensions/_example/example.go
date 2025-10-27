package example

import (
	"log/slog"
	"net/http"

	"github.com/InsulaLabs/insi/internal/badge"
	"github.com/InsulaLabs/insi/internal/db/core"
	"github.com/InsulaLabs/insi/pkg/extensions"
)

/*



 */

type Extension struct {
	logger  *slog.Logger
	insight core.EntityInsight
	panel   core.ExtensionPanel

	nodeIdentity badge.Badge
	nodeName     string
}

/*
	In order to be utilized over the ssh server, the example takes
	on the core.Exetension, allowing commands to be passed fromt the CLI
*/

var _ core.ExtensionControl = &Extension{}

func (e *Extension) CommandName() string {
	return "example"
}

func (e *Extension) GetHelpText() string {
	return "Example extension"
}

func (e *Extension) HandleCommand(command string, args []string) (string, error) {
	return "", nil
}

/*
	In order to qualify as an insi module, the example takes on
	the extensions.InisModule
*/

var _ extensions.InsiModule = &Extension{}

func (e *Extension) Name() string {
	return "example"
}

func (e *Extension) Version() string {
	return "1.0.0"
}

func (e *Extension) Description() string {
	return "Example extension"
}

func (e *Extension) GetController() core.ExtensionControl {
	return e
}

func (e *Extension) ReceiveInsightInterface(insight core.EntityInsight) {
	e.insight = insight

	/*
		The insight interface is used to retrieve entities and their data
		from the database. This is useful for extensions that need to access
		entity data for any reason.
	*/
}

func (e *Extension) OnInsiReady(panel core.ExtensionPanel) {
	e.panel = panel
	e.nodeIdentity = panel.GetNodeIdentity()
	e.nodeName = panel.GetNodeName()
	e.logger.Info("Insi ready", "node_name", e.nodeName, "node_id", e.nodeIdentity.GetID())
}

func (e *Extension) BindPublicRoutes(mux *http.ServeMux) {

	// Any routes mounter here will be on the public binding of the given insi config
}

func (e *Extension) BindPrivateRoutes(mux *http.ServeMux) {

	// Any routes mounter here will be on the private binding of the given insi config
}

func NewExtension(logger *slog.Logger) *Extension {
	return &Extension{
		logger: logger,
	}
}
