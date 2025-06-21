package core

import (
	"github.com/InsulaLabs/insi/db/models"
)

type EndpointOperation int

// Non-system-root entities
type Entity struct {
	RootApiKey    string   // the key we issue known as "their root" (of which aliases are made)
	Aliases       []string // the aliases they have made
	DataScopeUUID string   // the data scope they have created
	KeyUUID       string   // the key they have created
	Usage         models.LimitsResponse
}

type EntityInsight interface {
	GetEntity(rootApiKey string) (Entity, error)
	GetEntities(offset, limit int) ([]Entity, error)
	GetEntityByAlias(alias string) (Entity, error)
	GetEntityByDataScopeUUID(dataScopeUUID string) (Entity, error)
	GetEntityByKeyUUID(keyUUID string) (Entity, error)
}

type Insight interface {
	EntityInsight
}
