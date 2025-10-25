package models

const (
	DefaultRPSDataLimit  = 25
	DefaultRPSEventLimit = 10
)

type ApiKeyCreateRequest struct {
	KeyName string `json:"key_name"`
}

type ApiKeyCreateResponse struct {
	KeyName string `json:"key_name"`
	Key     string `json:"key"`
}

type ApiKeyDeleteRequest struct {
	Key string `json:"key"`
}

type TokenData struct {
	Entity        string `json:"e,omitempty"`
	DataScopeUUID string `json:"ds"`
	KeyUUID       string `json:"k"`
	IsAlias       bool   `json:"is_alias,omitempty"`
	Token         string `json:"-"` // The raw token string
}

type Limits struct {
	BytesOnDisk   *int64 `json:"bytes_on_disk,omitempty"`
	BytesInMemory *int64 `json:"bytes_in_memory,omitempty"`
	EventsEmitted *int64 `json:"events_emitted,omitempty"`
	Subscribers   *int64 `json:"subscribers,omitempty"`

	RPSDataLimit  *int64 `json:"rate_per_second_data_limit,omitempty"`  // cache blob and vs rates per second
	RPSEventLimit *int64 `json:"rate_per_second_event_limit,omitempty"` // event pub/sub rates per second
}

type LimitsResponse struct {
	CurrentUsage *Limits `json:"usage"`
	MaxLimits    *Limits `json:"max_limits"`
}

type SetLimitsRequest struct {
	ApiKey string  `json:"api_key"`
	Limits *Limits `json:"limits"`
}

type GetLimitsRequest struct {
	ApiKey string `json:"api_key"`
}

// NOTE: When i mention "root" below I DONT mean the "system root key" - I mean the "entity root key" aka the one made from spawnApiKey
// set alias is a get with the auth token header set tot he entity key (the api key) that the user wants to derive from

type SetAliasResponse struct {
	Alias string `json:"alias"` // The encrypted api key that acts as an alias to the root api key
}

// Deletes the alias - the authenticaor token to access must NOT be the alias and MUST be the entity-root key (not the system root key)
type DeleteAliasRequest struct {
	Alias string `json:"alias"` // The encrypted api key that acts as an alias to the root api key
}

// The request is just a get and we use the api key they auth with as the root
type ListAliasesResponse struct {
	Aliases []string `json:"aliases"` // returns names and keys for all aliases they have set
}
