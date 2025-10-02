package process

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/InsulaLabs/insi/badge"
	"github.com/InsulaLabs/insi/db/core"
	"github.com/InsulaLabs/insi/extensions"
	processModels "github.com/InsulaLabs/insi/extensions/process/models"
	"github.com/InsulaLabs/insi/extensions/process/procman"
	"github.com/google/uuid"
)

const (
	extensionName        = "process"
	extensionVersion     = "1.0.0"
	extensionDescription = "Process extension"
	extensionSaveFile    = "processes.json"
)

type Extension struct {
	logger  *slog.Logger
	insight core.EntityInsight
	panel   core.ExtensionPanel
	host    *procman.Host

	nodeIdentity badge.Badge
	nodeName     string

	processRegistry map[string]*processModels.Process
	registryMutex   sync.RWMutex

	procDir string
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

}

func (e *Extension) BindPrivateRoutes(mux *http.ServeMux) {

	// GET - offset and limit in url params
	mux.HandleFunc("/db/api/v1/extension/process/list", e.withMiddlwares(e.listProcesses))
	// POST
	mux.HandleFunc("/db/api/v1/extension/process/register", e.withMiddlwares(e.registerProcessHandler))
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

	e.procDir = filepath.Join(e.panel.GetNodeInstallDir(), "extensions", "process")
	if err := os.MkdirAll(e.procDir, 0755); err != nil {
		e.logger.Error("Failed to create process directory", "error", err)
		os.Exit(1)
	}

	e.mustRestore()
}

func (e *Extension) mustRestore() {
	procFile := filepath.Join(e.procDir, extensionSaveFile)
	if _, err := os.Stat(procFile); os.IsNotExist(err) {
		return
	}

	// we need to directly restore the fucking map

	file, err := os.Open(procFile)
	if err != nil {
		e.logger.Error("Failed to open process state file", "error", err)
		os.Exit(1)
	}
	defer file.Close()

	restoredMap := make(map[string]*processModels.Process)
	if err := json.NewDecoder(file).Decode(&restoredMap); err != nil {
		e.logger.Error("Failed to decode process state", "error", err)
		os.Exit(1)
	}

	e.processRegistry = restoredMap
	e.logger.Info("Process state restored", "process_registry", e.processRegistry)
}

func (e *Extension) save() error {
	procFile := filepath.Join(e.procDir, extensionSaveFile)
	file, err := os.Create(procFile)
	if err != nil {
		e.logger.Error("Failed to create process state file", "error", err)
		return err
	}
	defer file.Close()

	if err := json.NewEncoder(file).Encode(e.processRegistry); err != nil {
		e.logger.Error("Failed to encode process state", "error", err)
		return err
	}
	return nil
}

func NewExtension(logger *slog.Logger) *Extension {
	return &Extension{
		logger:          logger,
		host:            procman.NewHost(logger.WithGroup("procman")),
		processRegistry: make(map[string]*processModels.Process),
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
			proc.Status = processModels.ProcessStatusRunning
		} else {
			proc.Status = processModels.ProcessStatusStopped
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

	e.processRegistry[uuid] = &processModels.Process{
		UUID:     uuid,
		Name:     name,
		Status:   processModels.ProcessStatusStopped,
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

	if err := e.save(); err != nil {
		e.logger.Error("Failed to save process state", "error", err)
		return err
	}

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
	processes := make([]processModels.Process, 0, len(e.processRegistry))
	for _, proc := range e.processRegistry {
		processes = append(processes, *proc)
	}
	e.registryMutex.RUnlock()

	if offset >= len(processes) {
		processes = []processModels.Process{}
	} else {
		end := offset + limit
		if end > len(processes) {
			end = len(processes)
		}
		processes = processes[offset:end]
	}

	response := processModels.ProcessListResponse{
		Processes: processes,
		NodeInfo: processModels.ProcessNodeInfo{
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

	var cmd processModels.ProcessCommand
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

	// If already running, return success without starting again
	if e.processRegistry[cmd.UUID].Status == processModels.ProcessStatusRunning {
		response := processModels.ProcessCommandResponse{
			ReturnCode: 0,
			Message:    "Process already running",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	if err := e.host.StartApp(cmd.UUID); err != nil {
		e.logger.Error("failed to start process", "uuid", cmd.UUID, "error", err)
		response := processModels.ProcessCommandResponse{
			ReturnCode: 1,
			Message:    err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	response := processModels.ProcessCommandResponse{
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

	var cmd processModels.ProcessCommand
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

	// If not running, return success without stopping
	if proc.Status != processModels.ProcessStatusRunning {
		response := processModels.ProcessCommandResponse{
			ReturnCode: 0,
			Message:    "Process already stopped",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	if err := e.host.StopApp(cmd.UUID); err != nil {
		e.logger.Error("failed to stop process", "uuid", cmd.UUID, "error", err)
		response := processModels.ProcessCommandResponse{
			ReturnCode: 1,
			Message:    err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	response := processModels.ProcessCommandResponse{
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

	var cmd processModels.ProcessCommand
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

	// If not running, return success without restarting
	if proc.Status != processModels.ProcessStatusRunning {
		response := processModels.ProcessCommandResponse{
			ReturnCode: 0,
			Message:    "Process already stopped",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	if err := e.host.StopApp(cmd.UUID); err != nil {
		e.logger.Error("failed to stop process for restart", "uuid", cmd.UUID, "error", err)
	}

	if err := e.host.StartApp(cmd.UUID); err != nil {
		e.logger.Error("failed to start process after restart", "uuid", cmd.UUID, "error", err)
		response := processModels.ProcessCommandResponse{
			ReturnCode: 1,
			Message:    err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	response := processModels.ProcessCommandResponse{
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

	var cmd processModels.ProcessCommand
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

func (e *Extension) registerProcessHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req processModels.ProcessRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "Name is required", http.StatusBadRequest)
		return
	}
	if req.TargetPath == "" {
		http.Error(w, "TargetPath is required", http.StatusBadRequest)
		return
	}

	// Generate a unique UUID for this process
	uuid := uuid.New().String()

	err := e.registerProcess(uuid, req.Name, req.TargetPath, req.Args)
	if err != nil {
		e.logger.Error("failed to register process", "uuid", uuid, "error", err)
		response := processModels.ProcessRegisterResponse{
			Success: false,
			UUID:    "",
			Message: err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	response := processModels.ProcessRegisterResponse{
		Success: true,
		UUID:    uuid,
		Message: "Process registered successfully",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
