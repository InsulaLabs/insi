package static

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/InsulaLabs/insi/runtime"
)

/*

The server mounts plugins by name on "/" so if we have a plugin called "static"
then we can serve directly from "/static" automatically, and serve out the static
files from the dir handed to the plugin on creation.

ALlowing the server to server static files without modifying the internals
*/

type StaticPlugin struct {
	logger    *slog.Logger
	prif      runtime.ServiceRuntimeIF
	staticDir string
	startedAt time.Time
}

var _ runtime.Service = &StaticPlugin{}

func New(logger *slog.Logger, staticDir string) *StaticPlugin {
	return &StaticPlugin{
		logger:    logger,
		staticDir: staticDir,
	}
}

func (p *StaticPlugin) GetName() string {
	return "static"
}

func (p *StaticPlugin) Init(prif runtime.ServiceRuntimeIF) *runtime.ServiceImplError {
	p.prif = prif
	if _, err := os.Stat(p.staticDir); os.IsNotExist(err) {
		return &runtime.ServiceImplError{Err: fmt.Errorf("static directory does not exist, so plugin cannot be initialized: %s", p.staticDir)}
	}
	p.startedAt = time.Now()

	fs := http.FileServer(http.Dir(p.staticDir))
	err := p.prif.RT_MountStatic(p, fs)
	if err != nil {
		return &runtime.ServiceImplError{Err: fmt.Errorf("failed to mount static files: %w", err)}
	}
	return nil
}

func (p *StaticPlugin) GetRoutes() []runtime.ServiceRoute {
	// No routes for static plugin
	return []runtime.ServiceRoute{}
}
