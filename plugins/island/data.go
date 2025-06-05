package island

import "time"

type Island struct {
	Entity     string `json:"entity"`
	EntityUUID string `json:"entity_uuid"`

	UUID        string `json:"uuid"` // wwe generate
	Name        string `json:"name"`
	ModelSlug   string `json:"model_slug"`  // unique to entity must be valid URL
	Description string `json:"description"` // max 1024 chars

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ResourceType string

const (
	ResourceTypeTextFile     ResourceType = "text_file"
	ResourceTypeMarkdownFile ResourceType = "markdown_file"
	ResourceTypePDFFile      ResourceType = "pdf_file"
	ResourceTypeImage        ResourceType = "image"
	ResourceTypeSqliteDB     ResourceType = "sqlite-db"
	ResourceTypePostgresDB   ResourceType = "postgres-db"
)

/*
Using the OBJECT endpoint we can upload any file to the cluster
and we store some meta about it that is retrievable from the object endpoint using:

metaKey := fmt.Sprintf("plugin:objects:%s", objectFileUUID)

The meta data is stored and tyoped in the plugins/objects endpoint
*/
type Resource struct {
	Entity     string `json:"entity"`
	EntityUUID string `json:"entity_uuid"`

	UUID string       `json:"uuid"` // uuid of the object stored in the object endpoint
	Type ResourceType `json:"type"`

	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Description string    `json:"description"`
}

/// requests

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
	IslandUUID string     `json:"island_uuid"`
	Resources  []Resource `json:"resources"`
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
