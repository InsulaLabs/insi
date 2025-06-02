package models

// AtomicKeyPayload is used for operations that only need a key for atomics.
// (e.g., AtomicGet, AtomicDelete)
// It mirrors KeyPayload but is specific to atomic operations for clarity.
type AtomicKeyPayload struct {
	Key string `json:"key"`
}

// AtomicNewRequest is the payload for creating a new atomic counter.
type AtomicNewRequest struct {
	Key       string `json:"key"`
	Overwrite bool   `json:"overwrite,omitempty"` // Defaults to false if not provided
}

// AtomicAddRequest is the payload for adding to an atomic counter.
type AtomicAddRequest struct {
	Key   string `json:"key"`
	Delta int64  `json:"delta"`
}

// AtomicAddResponse is the response after successfully adding to an atomic counter.
// It returns the new value of the counter.
type AtomicAddResponse struct {
	Key      string `json:"key"`
	NewValue int64  `json:"new_value"`
}

// AtomicGetResponse is the response for getting the value of an atomic counter.
type AtomicGetResponse struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}
