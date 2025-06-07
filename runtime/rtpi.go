package runtime

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/config"
	"github.com/InsulaLabs/insi/db/core"
	db_models "github.com/InsulaLabs/insi/db/models"
)

// ------------------------------------------------------------
// PluginRuntimeIF implementation
// ------------------------------------------------------------

/*
	A note on implementation decsisions:

		Instead of having an interface that connects directlry to the
		service internals that can put directly to the raft fsm, i opted
		to use http clients to request onto the network. This is about
		distributing the load of requests from all node plugins across
		all nodes equally.

		In a once node cluster, this is a bit of a waste of resources, but
		consider the scale. In a 5 node cluster, inundating one server with
		requests can cause lots of problems. With the client-map setup we
		can distribute the load (reads) across all nodes

		Some functions like writing to the datastore will still need to
		locate the leader so the client takes care of this for us as well
		and we can also leverage the client for websocket subscription connections
		to remote nodes facilitating off-raft data transfer using the same
		code [client] that we have to maintain anyway (massive W)
*/

func (r *Runtime) RT_IsRunning() bool {
	return r.appCtx.Err() == nil
}

func (r *Runtime) RT_Set(kvp db_models.KVPayload) error {
	return r.rtClients["set"].Set(kvp.Key, kvp.Value)
}

func (r *Runtime) RT_Get(key string) (string, error) {
	return r.rtClients["get"].Get(key)
}

func (r *Runtime) RT_Delete(key string) error {
	return r.rtClients["delete"].Delete(key)
}

func (r *Runtime) RT_Iterate(prefix string, offset int, limit int) ([]string, error) {
	return r.rtClients["iterate"].IterateByPrefix(prefix, offset, limit)
}

func (r *Runtime) RT_SetCache(key string, value string, ttl time.Duration) error {
	return r.rtClients["setCache"].SetCache(key, value, ttl)
}

func (r *Runtime) RT_GetCache(key string) (string, error) {
	return r.rtClients["getCache"].GetCache(key)
}

func (r *Runtime) RT_DeleteCache(key string) error {
	return r.rtClients["deleteCache"].DeleteCache(key)
}

func (r *Runtime) RT_PublishEvent(topic string, data any) error {
	return r.rtClients["publishEvent"].PublishEvent(topic, data)
}

func (r *Runtime) RT_GetClientForToken(token string) (*client.Client, error) {
	return r.GetClientForToken(token)
}

// ------------------------------------------------------------
// PluginRuntimeIF implementation
// ------------------------------------------------------------

func (r *Runtime) RT_GetClusterConfig() *config.Cluster {
	return r.clusterCfg
}

// A Special case command made for static plugin (no better way - only exception)
func (r *Runtime) RT_MountStatic(caller Service, fs http.Handler) error {
	pluginName := strings.Trim(caller.GetName(), "/")
	if pluginName == "" {
		return fmt.Errorf("plugin name cannot be empty for mounting static files")
	}
	mountPathPrefix := fmt.Sprintf("/%s/", pluginName)

	strippedHandler := http.StripPrefix(mountPathPrefix, fs)
	return r.service.AddHandler(mountPathPrefix, strippedHandler)
}

func (r *Runtime) RT_ValidateAuthToken(req *http.Request, mustBeRoot bool) (db_models.TokenData, bool) {
	return r.service.ValidateToken(req, mustBeRoot)
}

func (r *Runtime) RT_IsRoot(td db_models.TokenData) bool {
	return td.Entity == core.EntityRoot && td.DataScopeUUID == r.clusterCfg.RootPrefix
}

func (r *Runtime) RT_GetNodeConfig() *config.Node {
	nodeCfg, ok := r.clusterCfg.Nodes[r.asNodeId]
	if !ok {
		return nil
	}
	return &nodeCfg
}

func (r *Runtime) RT_GetNodeID() string {
	return r.asNodeId
}
