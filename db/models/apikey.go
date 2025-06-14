package models

/*

	For simplicity sake and not assuming behavior intended, we track
	the amount of data each api key has in memory, on disk, and the number
	of events per second they've sent over the last 1 minute along with
	how many subscribers they have actively across all nodes

	These metrics are not internally used for limiting, but are used for tracking,
	if limiting is desired, it must be done in the network layer (not application layer.)
	Essentially, the thing using an insi client and a key must determine if they should
	make the request or not.
*/

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
	Entity        string `json:"e,omitempty"` // note: fields kept short intentionally
	DataScopeUUID string `json:"ds"`
	KeyUUID       string `json:"k"`
}
