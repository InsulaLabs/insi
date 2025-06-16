package models

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
}

type Limits struct {
	BytesOnDisk   *int64 `json:"bytes_on_disk,omitempty"`
	BytesInMemory *int64 `json:"bytes_in_memory,omitempty"`
	EventsEmitted *int64 `json:"events_emitted,omitempty"`
	Subscribers   *int64 `json:"subscribers,omitempty"`
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
