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
	prif      runtime.PluginRuntimeIF
	staticDir string
	startedAt time.Time
}

var _ runtime.Plugin = &StaticPlugin{}

func New(logger *slog.Logger, staticDir string) *StaticPlugin {
	return &StaticPlugin{
		logger:    logger,
		staticDir: staticDir,
	}
}

func (p *StaticPlugin) GetName() string {
	return "static"
}

func (p *StaticPlugin) Init(prif runtime.PluginRuntimeIF) *runtime.PluginImplError {
	p.prif = prif
	if _, err := os.Stat(p.staticDir); os.IsNotExist(err) {
		return &runtime.PluginImplError{Err: fmt.Errorf("static directory does not exist, so plugin cannot be initialized: %s", p.staticDir)}
	}
	p.startedAt = time.Now()

	fs := http.FileServer(http.Dir(p.staticDir))
	err := p.prif.RT_MountStatic(p, fs)
	if err != nil {
		return &runtime.PluginImplError{Err: fmt.Errorf("failed to mount static files: %w", err)}
	}

	return nil
}

func (p *StaticPlugin) GetRoutes() []runtime.PluginRoute {
	return []runtime.PluginRoute{
		/*
			IMPORTANT:
				DO NOT PREFIX THE PATH WITH TEH NAME OF THE PLUGIN

				THIS IS OF PARAMOUNT IMPORTANCE AS THE ROUTES ARE AUTOMATICALLY MOUNTED
				TO THE SERVICE PREFIXED BY THE NAME OF THE PLUGIN.
				ADDING "/service-name" HERE WILL CASUSE "/service-name/service-name/uptime"
				TO BE MOUNTED.
		*/
		{Path: "uptime", Handler: http.HandlerFunc(p.uptimeHandler), Limit: 10, Burst: 10},
	}
}

// / -------- routes --------
func (p *StaticPlugin) uptimeHandler(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	uptime := now.Sub(p.startedAt)

	// Set cache busting headers
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("Content-Type", "application/json")

	w.Write(
		[]byte(
			fmt.Sprintf(
				`{"status":"OK","startedAt":"%s","uptime":"%s"}`,
				p.startedAt.Format(time.RFC3339),
				uptime.String(),
			),
		),
	)
	w.Write([]byte("\n"))
	w.WriteHeader(http.StatusOK)
}
