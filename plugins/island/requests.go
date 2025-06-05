package island

import "github.com/InsulaLabs/insi/plugins"

type NewIslandRequest struct {
	Name        string `json:"name"`
	ModelSlug   string `json:"model_slug"`
	Description string `json:"description"`
}

type DeleteIslandRequest struct {
	UUID string `json:"uuid"`
}

type UpdateIslandNameRequest struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
}

type UpdateIslandDescriptionRequest struct {
	UUID        string `json:"uuid"`
	Description string `json:"description"`
}

type UpdateIslandModelSlugRequest struct {
	UUID      string `json:"uuid"`
	ModelSlug string `json:"model_slug"`
}

type UpdateIslandAddResourcesRequest struct {
	IslandUUID string             `json:"island_uuid"`
	Resources  []plugins.Resource `json:"resources"`
}

type UpdateIslandRemoveResourcesRequest struct {
	IslandUUID    string   `json:"island_uuid"`
	ResourceUUIDs []string `json:"resource_uuids"`
}

type IterateIslandResourcesRequest struct {
	IslandUUID string `json:"island_uuid"`
	Offset     int    `json:"offset"`
	Limit      int    `json:"limit"`
}
