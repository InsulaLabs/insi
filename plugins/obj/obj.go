package obj

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/InsulaLabs/insi/runtime"
)

// TODO: Creates an "object" layer over the k/v store that versions objects
// and handles migrations.

type ObjPlugin struct {
	logger *slog.Logger
	prif   runtime.PluginRuntimeIF

	startedAt time.Time
}

var _ runtime.Plugin = &ObjPlugin{}

func New(logger *slog.Logger) *ObjPlugin {
	return &ObjPlugin{
		logger: logger,
	}
}

func (p *ObjPlugin) GetName() string {
	return "obj"
}

func (p *ObjPlugin) Init(prif runtime.PluginRuntimeIF) *runtime.PluginImplError {
	p.prif = prif
	p.startedAt = time.Now()
	return nil
}

func (p *ObjPlugin) GetRoutes() []runtime.PluginRoute {
	return []runtime.PluginRoute{
		/*
			IMPORTANT:
				DO NOT PREFIX THE PATH WITH TEH NAME OF THE PLUGIN

				THIS IS OF PARAMOUNT IMPORTANCE AS THE ROUTES ARE AUTOMATICALLY MOUNTED
				TO THE SERVICE PREFIXED BY THE NAME OF THE PLUGIN.
				ADDING "/service-name" HERE WILL CASUSE "/service-name/service-name/uptime"
				TO BE MOUNTED.
		*/
		{Path: "new/schema", Handler: http.HandlerFunc(p.newSchemaHandler), Limit: 10, Burst: 10},
	}
}

// / -------- routes --------
func (p *ObjPlugin) newSchemaHandler(w http.ResponseWriter, r *http.Request) {

	td, ok := p.prif.RT_ValidateAuthToken(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// TODO: Implement
	fmt.Println(td)

}
