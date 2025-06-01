package runtime

import "net/http"

// The restricted interfaces that permit the plugin
// implementation to interact with the runtime.
type PluginRuntimeIF interface {
	IsRunning() bool
}

/*
Set of errors that the plugin implementation can return
to calls into the Plugin interface to inform the runtime
of specfic failures and hint towards possible recovery.
*/
type PluginImplError struct {
	Err error
}

func (e *PluginImplError) Error() string {
	return e.Err.Error()
}

func (e *PluginImplError) Unwrap() error {
	return e.Err
}

/*
The plugin interface is the entry point for the plugin
implementation to interact with the runtime.

Plugins are loaded at runtime and are expected to implement
this interface.

Plugins are expected to be loaded from the plugin directory.
*/

type PluginRoute struct {
	Path    string
	Limit   int // Rate limit for the route
	Burst   int // Burst limit for the route
	Handler http.Handler
}

/*
Plugins are mounted to:
	/plugin-name

Plugin paths are then mounted to

    /plugin-name/route-name

	   and the Handler is the http.Handler that will be used to handle the request.

Using the prif interface the route internals can interface with the runtime
to perform runtime operations upon request.
*/

type Plugin interface {
	GetName() string

	Init(prif PluginRuntimeIF) *PluginImplError

	// Routes open to public clients (even web)
	GetRoutes() []PluginRoute
}
