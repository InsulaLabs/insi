package plugins

import (
	"fmt"
	"strings"
	"time"
)

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

// Key helpers

func GetIslandKey(entityUUID, islandUUID string) string {
	return fmt.Sprintf("plugin:island:%s:%s", entityUUID, islandUUID)
}

func GetIslandIterationPrefix(entityUUID string) string {
	return fmt.Sprintf("plugin:island:%s:", entityUUID)
}

func GetIslandSlugKey(entityUUID, slug string) string {
	return fmt.Sprintf("plugin:island:slug:%s:%s", entityUUID, normalizeSlug(slug))
}

func GetIslandResourceKey(entityUUID, islandUUID, resourceUUID string) string {
	return fmt.Sprintf("plugin:island:%s:%s:resource:%s", entityUUID, islandUUID, resourceUUID)
}

func GetIslandResourceIterationPrefix(entityUUID, islandUUID string) string {
	return fmt.Sprintf("plugin:island:%s:%s:resource:", entityUUID, islandUUID)
}

func normalizeSlug(slug string) string {
	slug = strings.ReplaceAll(slug, " ", "-")
	slug = strings.ReplaceAll(slug, "_", "-")
	slug = strings.ReplaceAll(slug, ".", "-")
	slug = strings.ReplaceAll(slug, "/", "-")
	slug = strings.ReplaceAll(slug, "\\", "-")
	slug = strings.ReplaceAll(slug, "|", "-")
	return strings.ToLower(strings.TrimSpace(slug))
}
