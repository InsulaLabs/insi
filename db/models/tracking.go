package models

type Usage struct {
	MemoryUsage int `json:"memory_usage"`
	DiskUsage   int `json:"disk_usage"`
}
