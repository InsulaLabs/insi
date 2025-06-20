package models

import "time"

// Blob represents the metadata for a large binary object stored in the cluster.
type Blob struct {
	// Key is the user-defined identifier for the blob.
	Key string `json:"key"`
	// OwnerUUID is the UUID of the API key that owns this blob.
	OwnerUUID string `json:"owner_uuid"`
	// DataScopeUUID is the data scope for this blob, tied to the owner.
	DataScopeUUID string `json:"data_scope_uuid"`
	// NodeID is the ID of the node where the blob was originally uploaded.
	NodeID string `json:"node_id"`

	// NodeIdentityUUID is the UUID of the node that owns this blob.
	NodeIdentityUUID string `json:"node_identity_uuid"`

	// Size is the size of the blob in bytes.
	Size int64 `json:"size"`
	// Hash is the SHA256 hash of the blob's content.
	Hash string `json:"hash"`
	// UploadedAt is the timestamp of when the blob was uploaded.
	UploadedAt time.Time `json:"uploaded_at"`
	// OriginalName is the original filename of the uploaded file.
	OriginalName string `json:"original_name,omitempty"`
}
