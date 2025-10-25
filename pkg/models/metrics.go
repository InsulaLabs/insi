package models

// OpsPerSecondCounters holds the calculated operations per second for various services.
type OpsPerSecondCounters struct {
	OP_VS          float64 `json:"op_vs"`
	OP_Cache       float64 `json:"op_cache"`
	OP_Events      float64 `json:"op_events"`
	OP_Subscribers float64 `json:"op_subscribers"`
	OP_Blobs       float64 `json:"op_blobs"`
	OP_System      float64 `json:"op_system"`
}
