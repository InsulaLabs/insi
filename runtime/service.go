package runtime

import (
	"net/http"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	db_models "github.com/InsulaLabs/insi/db/models"
)

type ValueStoreIF interface {
	RT_Set(kvp db_models.KVPayload) error
	RT_Get(key string) (string, error)
	RT_Delete(key string) error
	RT_Iterate(prefix string, offset int, limit int) ([]string, error)
}

type CacheStoreIF interface {
	RT_SetCache(key string, value string) error
	RT_GetCache(key string) (string, error)
	RT_DeleteCache(key string) error
}

type EventStoreIF interface {
	RT_PublishEvent(topic string, data any) error
}

type WebServerIF interface {
	RT_MountStatic(caller Service, fs http.Handler) error
	RT_ValidateAuthToken(req *http.Request, mustBeRoot bool) (db_models.TokenData, bool)
	RT_IsRoot(db_models.TokenData) bool
}

// The restricted interfaces that permit the service
// implementation to interact with the runtime.
type ServiceRuntimeIF interface {
	RT_IsRunning() bool
	RT_GetClusterConfig() *config.Cluster
	RT_GetNodeConfig() *config.Node
	RT_GetNodeID() string
	RT_GetClientForToken(token string) (*client.Client, error)

	ValueStoreIF
	CacheStoreIF
	EventStoreIF
	WebServerIF
}

/*
Set of errors that the service implementation can return
to calls into the Service interface to inform the runtime
of specfic failures and hint towards possible recovery.
*/
type ServiceImplError struct {
	Err error
}

func (e *ServiceImplError) Error() string {
	return e.Err.Error()
}

func (e *ServiceImplError) Unwrap() error {
	return e.Err
}

/*
The service interface is the entry point for the service
implementation to interact with the runtime.

Services are loaded at runtime and are expected to implement
this interface.

Services are expected to be loaded from the service directory.
*/

type ServiceRoute struct {
	Path    string
	Limit   int // Rate limit for the route
	Burst   int // Burst limit for the route
	Handler http.Handler
}

/*
Services are mounted to:
	/service-name

Service paths are then mounted to

    /service-name/route-name

	   and the Handler is the http.Handler that will be used to handle the request.

Using the ServiceRuntimeIF interface the route internals can interface with the runtime
to perform runtime operations upon request.
*/

type Service interface {

	// Used to mount the service to the runtime.
	// and must be unique to the mounted services.
	GetName() string

	// Inform the service that the runtime is about to start
	// and to be ready to handle requests.
	Init(sif ServiceRuntimeIF) *ServiceImplError

	// Get all of the http routes and their rate limit specifications
	// for the service.
	// We _could_ allow them to limit themselves but if we force
	// them to specify we know they will defintely be limited (good.)
	GetRoutes() []ServiceRoute
}
