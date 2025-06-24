package models

/*
	Payloads for the various KV, Key, and Cache operations.
	These are all prefixed by the caller's api key unique identifier.
	Contextually seperating the accessable keys, and caches for each
	api key.
*/

type KVPayload struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KeyPayload struct {
	Key string `json:"key"`
}

// CASPayload represents the payload for a Compare-And-Swap operation.
type CASPayload struct {
	Key      string `json:"key"`
	OldValue string `json:"old_value"`
	NewValue string `json:"new_value"`
}

func (p *KVPayload) KeyLength() int {
	return len(p.Key)
}

func (p *KVPayload) ValueLength() int {
	return len(p.Value)
}

func (p *KVPayload) TotalLength() int {
	return p.KeyLength() + p.ValueLength()
}
