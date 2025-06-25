package models

type Entity struct {
	RootApiKey    string         `json:"root_api_key"`    // the key we issue known as "their root" (of which aliases are made)
	Aliases       []string       `json:"aliases"`         // the aliases they have made
	DataScopeUUID string         `json:"data_scope_uuid"` // the data scope they have created
	KeyUUID       string         `json:"key_uuid"`        // the key they have created
	Usage         LimitsResponse `json:"usage"`
}

type InsightRequestEntity struct {
	RootApiKey string `json:"root_api_key"`
}

type InsightResponseEntity struct {
	Entity Entity `json:"entity"`
}

type InsightRequestEntities struct {
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

type InsightResponseEntities struct {
	Entities []Entity `json:"entities"`
}

type InsightRequestEntityByAlias struct {
	Alias string `json:"alias"`
}

type InsightResponseEntityByAlias struct {
	Entity Entity `json:"entity"`
}

// this is all we are exposing of the insight api into core (for security reasons)
