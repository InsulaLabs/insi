package process

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"

	"github.com/InsulaLabs/insi/badge"
	"github.com/InsulaLabs/insi/db/core"
	"github.com/InsulaLabs/insi/extensions"
	"github.com/InsulaLabs/insi/extensions/process/procman"
)

const (
	extensionName        = "process"
	extensionVersion     = "1.0.0"
	extensionDescription = "Process extension"
)

type Extension struct {
	logger     *slog.Logger
	rootApiKey string
	insight    core.EntityInsight
	panel      core.ExtensionPanel
	host       *procman.Host

	nodeIdentity badge.Badge
	nodeName     string

	processRegistry map[string]*Process
	registryMutex   sync.RWMutex
}

var _ extensions.InsiModule = &Extension{}

func (e *Extension) Name() string {
	return extensionName
}

func (e *Extension) Version() string {
	return extensionVersion
}

func (e *Extension) Description() string {
	return extensionDescription
}

func (e *Extension) ReceiveInsightInterface(insight core.EntityInsight) {
	e.insight = insight
}

func (e *Extension) BindPublicRoutes(mux *http.ServeMux) {

	// GET - offset and limit in url params
	mux.HandleFunc("/db/api/v1/extension/process/list", e.withMiddlwares(e.listProcesses))
}

func (e *Extension) BindPrivateRoutes(mux *http.ServeMux) {

	// POST
	mux.HandleFunc("/db/api/v1/extension/process/start", e.withMiddlwares(e.startProcess))
	mux.HandleFunc("/db/api/v1/extension/process/stop", e.withMiddlwares(e.stopProcess))
	mux.HandleFunc("/db/api/v1/extension/process/restart", e.withMiddlwares(e.restartProcess))
	mux.HandleFunc("/db/api/v1/extension/process/status", e.withMiddlwares(e.statusProcess))
}

func (e *Extension) OnInsiReady(panel core.ExtensionPanel) {
	e.panel = panel
	e.nodeIdentity = panel.GetNodeIdentity()
	e.nodeName = panel.GetNodeName()
	e.logger.Info("Insi ready", "node_name", e.nodeName, "node_id", e.nodeIdentity.GetID())
}

func NewExtension(logger *slog.Logger, rootApiKey string) *Extension {
	return &Extension{
		logger:          logger,
		rootApiKey:      rootApiKey,
		host:            procman.NewHost(logger.WithGroup("procman")),
		processRegistry: make(map[string]*Process),
	}
}

func (e *Extension) withMiddlwares(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ipFilter := e.panel.GetExtensionMiddlwares().GetIPPublicFilterMiddleware()

		tokenAuth := func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, ok := e.panel.ValidateToken(r, core.AccessEntityRoot)
				if !ok {
					http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
			})
		}

		handler := ipFilter(tokenAuth(http.HandlerFunc(next)))
		handler.ServeHTTP(w, r)
	}
}

func (e *Extension) updateProcessStatus(uuid string, running bool) {
	e.registryMutex.Lock()
	defer e.registryMutex.Unlock()

	if proc, exists := e.processRegistry[uuid]; exists {
		if running {
			proc.Status = ProcessStatusRunning
		} else {
			proc.Status = ProcessStatusStopped
		}
		e.logger.Info("process status updated", "uuid", uuid, "running", running)
	}
}

func (e *Extension) registerProcess(uuid, name, targetPath string, args []string) error {
	e.registryMutex.Lock()
	defer e.registryMutex.Unlock()

	if _, exists := e.processRegistry[uuid]; exists {
		return nil
	}

	e.processRegistry[uuid] = &Process{
		UUID:     uuid,
		Name:     name,
		Status:   ProcessStatusStopped,
		NodeName: e.nodeName,
		NodeID:   e.nodeIdentity.GetID(),
	}

	app := procman.NewHostedApp(procman.HostedAppOpt{
		Name:       uuid,
		TargetPath: targetPath,
		CmdArgs:    args,
		CmdOptions: procman.DefaultOptions(),
		StartStopCb: func(name string, running bool) {
			e.updateProcessStatus(name, running)
		},
	})

	e.host.WithApp(app)
	e.logger.Info("process registered", "uuid", uuid, "name", name)
	return nil
}

func (e *Extension) listProcesses(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	offset := 0
	limit := 100

	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	e.registryMutex.RLock()
	processes := make([]Process, 0, len(e.processRegistry))
	for _, proc := range e.processRegistry {
		processes = append(processes, *proc)
	}
	e.registryMutex.RUnlock()

	if offset >= len(processes) {
		processes = []Process{}
	} else {
		end := offset + limit
		if end > len(processes) {
			end = len(processes)
		}
		processes = processes[offset:end]
	}

	response := MsgListProcessResponse{
		Processes: processes,
		NodeInfo: NodeInfo{
			NodeName: e.nodeName,
			NodeID:   e.nodeIdentity.GetID(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		e.logger.Error("failed to encode response", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

func (e *Extension) startProcess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cmd MsgProcCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if cmd.UUID == "" {
		http.Error(w, "UUID is required", http.StatusBadRequest)
		return
	}

	e.registryMutex.RLock()
	_, exists := e.processRegistry[cmd.UUID]
	e.registryMutex.RUnlock()

	if !exists {
		http.Error(w, "Process not found", http.StatusNotFound)
		return
	}

	if err := e.host.StartApp(cmd.UUID); err != nil {
		e.logger.Error("failed to start process", "uuid", cmd.UUID, "error", err)
		response := MsgProcCommandResponse{
			ReturnCode: 1,
			Message:    err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	response := MsgProcCommandResponse{
		ReturnCode: 0,
		Message:    "Process started successfully",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (e *Extension) stopProcess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cmd MsgProcCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if cmd.UUID == "" {
		http.Error(w, "UUID is required", http.StatusBadRequest)
		return
	}

	e.registryMutex.RLock()
	_, exists := e.processRegistry[cmd.UUID]
	e.registryMutex.RUnlock()

	if !exists {
		http.Error(w, "Process not found", http.StatusNotFound)
		return
	}

	if err := e.host.StopApp(cmd.UUID); err != nil {
		e.logger.Error("failed to stop process", "uuid", cmd.UUID, "error", err)
		response := MsgProcCommandResponse{
			ReturnCode: 1,
			Message:    err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	response := MsgProcCommandResponse{
		ReturnCode: 0,
		Message:    "Process stopped successfully",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (e *Extension) restartProcess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cmd MsgProcCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if cmd.UUID == "" {
		http.Error(w, "UUID is required", http.StatusBadRequest)
		return
	}

	e.registryMutex.RLock()
	_, exists := e.processRegistry[cmd.UUID]
	e.registryMutex.RUnlock()

	if !exists {
		http.Error(w, "Process not found", http.StatusNotFound)
		return
	}

	if err := e.host.StopApp(cmd.UUID); err != nil {
		e.logger.Error("failed to stop process for restart", "uuid", cmd.UUID, "error", err)
	}

	if err := e.host.StartApp(cmd.UUID); err != nil {
		e.logger.Error("failed to start process after restart", "uuid", cmd.UUID, "error", err)
		response := MsgProcCommandResponse{
			ReturnCode: 1,
			Message:    err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	response := MsgProcCommandResponse{
		ReturnCode: 0,
		Message:    "Process restarted successfully",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (e *Extension) statusProcess(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cmd MsgProcCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if cmd.UUID == "" {
		http.Error(w, "UUID is required", http.StatusBadRequest)
		return
	}

	e.registryMutex.RLock()
	proc, exists := e.processRegistry[cmd.UUID]
	e.registryMutex.RUnlock()

	if !exists {
		http.Error(w, "Process not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(*proc)
}
