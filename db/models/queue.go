package models

// QueueKeyPayload is used for operations that only need a queue key.
type QueueKeyPayload struct {
	Key string `json:"key"`
}

// QueueNewRequest is the payload for creating a new queue.
// It's essentially the same as QueueKeyPayload but named for clarity.
type QueueNewRequest struct {
	Key string `json:"key"`
}

// QueuePushRequest is the payload for pushing a value to a queue.
type QueuePushRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// QueuePushResponse is the response after successfully pushing to a queue.
type QueuePushResponse struct {
	Key       string `json:"key"`
	NewLength int    `json:"new_length"`
}

// QueuePopResponse is the response after successfully popping from a queue.
type QueuePopResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// QueueDeleteRequest is the payload for deleting a queue.
// It's essentially the same as QueueKeyPayload but named for clarity.
type QueueDeleteRequest struct {
	Key string `json:"key"`
}
