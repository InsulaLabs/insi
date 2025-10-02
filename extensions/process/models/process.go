package models

type ProcessStatus string

const (
	ProcessStatusRunning  ProcessStatus = "running"
	ProcessStatusStarting ProcessStatus = "starting"
	ProcessStatusError    ProcessStatus = "error"
	ProcessStatusStopped  ProcessStatus = "stopped"
)

type Process struct {
	UUID     string        `json:"uuid"`
	Name     string        `json:"name"`
	Status   ProcessStatus `json:"status"`
	NodeName string        `json:"node_name"`
	NodeID   string        `json:"node_id"`
}

type ProcessNodeInfo struct {
	NodeName string `json:"node_name"`
	NodeID   string `json:"node_id"`
}

type ProcessListResponse struct {
	Processes []Process       `json:"processes"`
	NodeInfo  ProcessNodeInfo `json:"node_info"`
}

type ProcessCommand struct {
	UUID string   `json:"uuid"`
	Args []string `json:"args,omitempty"`
}

type ProcessCommandResponse struct {
	ReturnCode int    `json:"return_code"`
	Message    string `json:"message"`
}

type ProcessRegisterRequest struct {
	Name       string   `json:"name"`
	TargetPath string   `json:"target_path"`
	Args       []string `json:"args,omitempty"`
}

type ProcessRegisterResponse struct {
	Success bool   `json:"success"`
	UUID    string `json:"uuid"`
	Message string `json:"message"`
}
