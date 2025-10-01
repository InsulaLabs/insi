package process

type ProcessStatus string

const (
	ProcessStatusRunning  ProcessStatus = "running"
	ProcessStatusStarting ProcessStatus = "starting"
	ProcessStatusError    ProcessStatus = "error"
	ProcessStatusStopped  ProcessStatus = "stopped"
)

type Process struct {
	UUID     string        `json:"uuid"` // internally maps to proman identification
	Name     string        `json:"name"`
	Status   ProcessStatus `json:"status"`
	NodeName string        `json:"node_name"` // which node this process is running on
	NodeID   string        `json:"node_id"`   // node identity ID
}

type NodeInfo struct {
	NodeName string `json:"node_name"`
	NodeID   string `json:"node_id"`
}

type MsgListProcessResponse struct {
	Processes []Process `json:"processes"`
	NodeInfo  NodeInfo  `json:"node_info"`
}

type MsgProcCommand struct {
	UUID string `json:"uuid"`

	// Optional input arguments
	Args []string `json:"args,omitempty"`
}

type MsgProcCommandResponse struct {
	ReturnCode int    `json:"return_code"`
	Message    string `json:"message"`
}
