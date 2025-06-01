package runtime

import (
	"time"

	"github.com/InsulaLabs/insi/models"
)

// ------------------------------------------------------------
// PluginRuntimeIF implementation
// ------------------------------------------------------------

func (r *Runtime) RT_IsRunning() bool {
	return r.appCtx.Err() == nil
}

func (r *Runtime) RT_Set(kvp models.KVPayload) error {
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

func (r *Runtime) RT_SetObject(key string, object []byte) error {
	return r.rtClients["setObject"].SetObject(key, object)
}

func (r *Runtime) RT_GetObject(key string) ([]byte, error) {
	return r.rtClients["getObject"].GetObject(key)
}

func (r *Runtime) RT_DeleteObject(key string) error {
	return r.rtClients["deleteObject"].DeleteObject(key)
}

func (r *Runtime) RT_IterateObject(prefix string, offset int, limit int) ([]string, error) {
	return r.rtClients["iterateObject"].IterateByPrefix(prefix, offset, limit)
}

func (r *Runtime) RT_GetObjectList(prefix string, offset int, limit int) ([]string, error) {
	return r.rtClients["getObjectList"].IterateByPrefix(prefix, offset, limit)
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
