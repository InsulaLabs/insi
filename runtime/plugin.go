package runtime

import (
	"net/http"
	"time"

	"github.com/InsulaLabs/insi/models"
)

type ValueStoreIF interface {
	RT_Set(kvp models.KVPayload) error
	RT_Get(key string) (string, error)
	RT_Delete(key string) error
	RT_Iterate(prefix string, offset int, limit int) ([]string, error)
}

type ObjectStoreIF interface {
	RT_SetObject(key string, object []byte) error
	RT_GetObject(key string) ([]byte, error)
	RT_DeleteObject(key string) error
	RT_IterateObject(prefix string, offset int, limit int) ([]string, error)
	RT_GetObjectList(prefix string, offset int, limit int) ([]string, error)
}

type CacheStoreIF interface {
	RT_SetCache(key string, value string, ttl time.Duration) error
	RT_GetCache(key string) (string, error)
	RT_DeleteCache(key string) error
}

type EventStoreIF interface {
	RT_PublishEvent(topic string, data any) error
}

// The restricted interfaces that permit the plugin
// implementation to interact with the runtime.
type PluginRuntimeIF interface {
	RT_IsRunning() bool

	ValueStoreIF
	ObjectStoreIF
	CacheStoreIF
	EventStoreIF
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

	// Used to mount the plugin to the runtime.
	// and must be unique to the mounted plugins.
	GetName() string

	// Inform the plugin that the runtime is about to start
	// and to be ready to handle requests.
	Init(prif PluginRuntimeIF) *PluginImplError

	// Get all of the http routes and their rate limit specifications
	// for the plugin.
	// We _could_ allow them to limit themselves but if we force
	// them to specify we know they will defintely be limited (good.)
	GetRoutes() []PluginRoute
}
