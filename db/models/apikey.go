package models

// -- Only root key can c/d api keys --

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

// Internal storage of the token data for the api key
type TokenData struct {
	Entity string `json:"entity"`
	UUID   string `json:"uuid"`
}
