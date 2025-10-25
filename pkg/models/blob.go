package models

import "time"

// Blob represents metadata for large binary objects stored in the cluster. It contains
// information about the blob's ownership (OwnerUUID, DataScopeUUID), location (NodeID,
// NodeIdentityUUID), and properties (Size, Hash, UploadedAt, OriginalName). The Key
// field serves as a user-defined identifier.
type Blob struct {
	Key              string    `json:"key"`
	OwnerUUID        string    `json:"owner_uuid"`
	DataScopeUUID    string    `json:"data_scope_uuid"`
	NodeID           string    `json:"node_id"`
	NodeIdentityUUID string    `json:"node_identity_uuid"`
	Size             int64     `json:"size"`
	Hash             string    `json:"hash"`
	UploadedAt       time.Time `json:"uploaded_at"`
	OriginalName     string    `json:"original_name,omitempty"`
}
