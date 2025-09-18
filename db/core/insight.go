package core

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/db/tkv"
)

type EndpointOperation int

// Non-system-root entities

type EntityInsight interface {
	GetEntity(rootApiKey string) (models.Entity, error)
	GetEntities(offset, limit int) ([]models.Entity, error)
	GetEntityByAlias(alias string) (models.Entity, error)
	GetEntityByDataScopeUUID(dataScopeUUID string) (models.Entity, error)
	GetEntityByKeyUUID(keyUUID string) (models.Entity, error)
}

type Insight interface {
	EntityInsight
}

const (
	apiKeyDataScopePrefix = "internal:api_key_ds"
	apiKeyRefPrefix       = "internal:api_key_ref"
)

func withApiKeyDataScope(keyUUID string) string {
	return apiKeyDataScopePrefix + ":" + keyUUID
}

func withApiKeyRef(keyUUID string) string {
	return apiKeyRefPrefix + ":" + keyUUID
}

func (c *Core) getEntity(keyUUID, apiKey, dataScopeUUID string) (models.Entity, error) {
	prefix := WithRootToAliasPrefix(keyUUID)
	aliasValues, err := c.fsm.Iterate(prefix, 0, MaxAliasesPerKey, "")
	if err != nil {
		var nfErr *tkv.ErrKeyNotFound
		if !errors.As(err, &nfErr) {
			c.logger.Warn("could not iterate aliases for entity", "key_uuid", keyUUID, "error", err)
		}
	}
	aliases := make([]string, len(aliasValues))
	for i, val := range aliasValues {
		fullKey := string(val)
		aliases[i] = strings.TrimPrefix(fullKey, prefix+":")
	}

	var usage models.LimitsResponse
	usage.CurrentUsage = &models.Limits{}
	usage.MaxLimits = &models.Limits{}

	getUsagePtr := func(key string, defaultVal int) *int64 {
		valStr, err := c.fsm.Get(key)
		var val int64
		if err != nil {
			val = int64(defaultVal)
		} else {
			val, _ = strconv.ParseInt(valStr, 10, 64)
		}
		return &val
	}

	usage.CurrentUsage.BytesInMemory = getUsagePtr(WithApiKeyMemoryUsage(dataScopeUUID), 0)
	usage.MaxLimits.BytesInMemory = getUsagePtr(WithApiKeyMaxMemoryUsage(dataScopeUUID), ApiDefaultMaxMemoryUsage)
	usage.CurrentUsage.BytesOnDisk = getUsagePtr(WithApiKeyDiskUsage(dataScopeUUID), 0)
	usage.MaxLimits.BytesOnDisk = getUsagePtr(WithApiKeyMaxDiskUsage(dataScopeUUID), ApiDefaultMaxDiskUsage)
	usage.CurrentUsage.EventsEmitted = getUsagePtr(WithApiKeyEvents(dataScopeUUID), 0)
	usage.MaxLimits.EventsEmitted = getUsagePtr(WithApiKeyMaxEvents(dataScopeUUID), ApiDefaultMaxEvents)
	usage.CurrentUsage.Subscribers = getUsagePtr(WithApiKeySubscriptions(dataScopeUUID), 0)
	usage.MaxLimits.Subscribers = getUsagePtr(WithApiKeyMaxSubscriptions(dataScopeUUID), ApiDefaultMaxSubscriptions)

	entity := models.Entity{
		RootApiKey:    apiKey,
		Aliases:       aliases,
		DataScopeUUID: dataScopeUUID,
		KeyUUID:       keyUUID,
		Usage:         usage,
	}
	return entity, nil
}

var _ EntityInsight = &Core{}

// rootApiKey is the non-alias non-system-root api key for the entity
func (c *Core) GetEntity(rootApiKey string) (models.Entity, error) {
	td, err := c.decomposeKey(rootApiKey)
	if err != nil {
		return models.Entity{}, errors.New("invalid api key")
	}

	if td.IsAlias {
		return models.Entity{}, errors.New("api key is an alias, not a root key")
	}

	return c.getEntity(td.KeyUUID, rootApiKey, td.DataScopeUUID)
}

func (c *Core) GetEntities(offset, limit int) ([]models.Entity, error) {

	// Use the apiKeyRefPrefix to get the actual Key UUID. Others use data scope uuid which can not be mapped back to a key.
	keys, err := c.fsm.Iterate(apiKeyRefPrefix, offset, limit, "")
	if err != nil {
		return nil, err
	}

	var entities []models.Entity
	for _, key := range keys {
		keyUUID := strings.TrimPrefix(key, apiKeyRefPrefix+":")
		entity, err := c.GetEntityByKeyUUID(keyUUID)
		if err != nil {
			c.logger.Warn("could not get entity for key uuid during iteration", "key_uuid", keyUUID, "error", err)
			continue
		}
		entities = append(entities, entity)
	}

	return entities, nil
}

func (c *Core) GetEntityByAlias(alias string) (models.Entity, error) {
	rootKeyUUID, err := c.fsm.Get(WithAliasToRoot(alias))
	if err != nil {
		return models.Entity{}, errors.New("alias not found")
	}

	return c.GetEntityByKeyUUID(rootKeyUUID)
}

func (c *Core) GetEntityByDataScopeUUID(dataScopeUUID string) (models.Entity, error) {
	keys, err := c.fsm.Iterate(apiKeyDataScopePrefix, 0, 1_000_000, "") // A reasonable limit
	if err != nil {
		return models.Entity{}, err
	}

	for _, key := range keys {
		dsUUID, err := c.fsm.Get(key)
		if err != nil {
			continue
		}
		if dsUUID == dataScopeUUID {
			keyUUID := strings.TrimPrefix(key, apiKeyDataScopePrefix+":")
			return c.GetEntityByKeyUUID(keyUUID)
		}
	}

	return models.Entity{}, errors.New("entity not found")
}

func (c *Core) GetEntityByKeyUUID(keyUUID string) (models.Entity, error) {
	apiKey, err := c.fsm.Get(withApiKeyRef(keyUUID))
	if err != nil {
		return models.Entity{}, errors.New("api key not found for key uuid")
	}

	dataScopeUUID, err := c.fsm.Get(withApiKeyDataScope(keyUUID))
	if err != nil {
		return models.Entity{}, errors.New("datascope not found for key uuid")
	}

	return c.getEntity(keyUUID, apiKey, dataScopeUUID)
}

func (c *Core) getEntityHandler(w http.ResponseWriter, r *http.Request) {
	c.IndSystemOp()

	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req models.InsightRequestEntity
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	entity, err := c.GetEntity(req.RootApiKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	resp := models.InsightResponseEntity{Entity: entity}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		c.logger.Error("failed to encode response", "error", err)
	}
}

func (c *Core) getEntitiesHandler(w http.ResponseWriter, r *http.Request) {
	c.IndSystemOp()

	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req models.InsightRequestEntities
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	entities, err := c.GetEntities(req.Offset, req.Limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := models.InsightResponseEntities{Entities: entities}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		c.logger.Error("failed to encode response", "error", err)
	}
}

func (c *Core) getEntityByAliasHandler(w http.ResponseWriter, r *http.Request) {
	c.IndSystemOp()

	_, ok := c.ValidateToken(r, RootOnly())
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req models.InsightRequestEntityByAlias
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	entity, err := c.GetEntityByAlias(req.Alias)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	resp := models.InsightResponseEntityByAlias{Entity: entity}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		c.logger.Error("failed to encode response", "error", err)
	}
}
