package process

import (
	"fmt"
	"strconv"

	processModels "github.com/InsulaLabs/insi/pkg/extensions/process/models"
	"github.com/google/uuid"
)

func (e *Extension) resolveProcessIdentifier(identifier string) (string, *processModels.Process, error) {
	e.registryMutex.RLock()
	defer e.registryMutex.RUnlock()

	if proc, exists := e.processRegistry[identifier]; exists {
		return identifier, proc, nil
	}

	if uuid, exists := e.nameToUUID[identifier]; exists {
		if proc, exists := e.processRegistry[uuid]; exists {
			return uuid, proc, nil
		}
	}

	return "", nil, fmt.Errorf("process not found: %s", identifier)
}

func (e *Extension) cliListProcesses(args []string) (string, error) {
	offset := 0
	limit := 100

	if len(args) > 0 {
		if o, err := strconv.Atoi(args[0]); err == nil && o >= 0 {
			offset = o
		}
	}

	if len(args) > 1 {
		if l, err := strconv.Atoi(args[1]); err == nil && l > 0 && l <= 1000 {
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

	return formatProcessList(processes, e.nodeName, e.nodeIdentity.GetID()), nil
}

func (e *Extension) cliRegisterProcessHandler(args []string) (string, error) {
	if len(args) < 2 {
		return formatCommandUsage("register", "<name> <target_path> [args...]"), fmt.Errorf("insufficient arguments")
	}

	name := args[0]
	targetPath := args[1]
	procArgs := []string{}

	if len(args) > 2 {
		procArgs = args[2:]
	}

	processUUID := uuid.New().String()

	err := e.registerProcess(processUUID, name, targetPath, procArgs)
	if err != nil {
		e.logger.Error("failed to register process", "uuid", processUUID, "error", err)
		return formatError(fmt.Sprintf("Failed to register process: %s", err.Error())), err
	}

	return formatSuccess(fmt.Sprintf("Process registered successfully\n  UUID: %s\n  Name: %s\n  Path: %s", processUUID, name, targetPath)), nil
}

func (e *Extension) cliStartProcess(args []string) (string, error) {
	if len(args) < 1 {
		return formatCommandUsage("start", "<name|uuid>"), fmt.Errorf("process name or uuid required")
	}

	identifier := args[0]

	processUUID, proc, err := e.resolveProcessIdentifier(identifier)
	if err != nil {
		return formatError(err.Error()), err
	}

	if proc.Status == processModels.ProcessStatusRunning {
		return formatSuccess("Process already running"), nil
	}

	if err := e.host.StartApp(processUUID); err != nil {
		e.logger.Error("failed to start process", "uuid", processUUID, "error", err)
		return formatError(fmt.Sprintf("Failed to start process: %s", err.Error())), err
	}

	return formatSuccess(fmt.Sprintf("Process started: %s (%s)", proc.Name, processUUID)), nil
}

func (e *Extension) cliStopProcess(args []string) (string, error) {
	if len(args) < 1 {
		return formatCommandUsage("stop", "<name|uuid>"), fmt.Errorf("process name or uuid required")
	}

	identifier := args[0]

	processUUID, proc, err := e.resolveProcessIdentifier(identifier)
	if err != nil {
		return formatError(err.Error()), err
	}

	if proc.Status != processModels.ProcessStatusRunning {
		return formatSuccess("Process already stopped"), nil
	}

	if err := e.host.StopApp(processUUID); err != nil {
		e.logger.Error("failed to stop process", "uuid", processUUID, "error", err)
		return formatError(fmt.Sprintf("Failed to stop process: %s", err.Error())), err
	}

	return formatSuccess(fmt.Sprintf("Process stopped: %s (%s)", proc.Name, processUUID)), nil
}

func (e *Extension) cliRestartProcess(args []string) (string, error) {
	if len(args) < 1 {
		return formatCommandUsage("restart", "<name|uuid>"), fmt.Errorf("process name or uuid required")
	}

	identifier := args[0]

	processUUID, proc, err := e.resolveProcessIdentifier(identifier)
	if err != nil {
		return formatError(err.Error()), err
	}

	if proc.Status != processModels.ProcessStatusRunning {
		return formatSuccess("Process is not running, cannot restart"), nil
	}

	if err := e.host.StopApp(processUUID); err != nil {
		e.logger.Error("failed to stop process for restart", "uuid", processUUID, "error", err)
	}

	if err := e.host.StartApp(processUUID); err != nil {
		e.logger.Error("failed to start process after restart", "uuid", processUUID, "error", err)
		return formatError(fmt.Sprintf("Failed to restart process: %s", err.Error())), err
	}

	return formatSuccess(fmt.Sprintf("Process restarted: %s (%s)", proc.Name, processUUID)), nil
}

func (e *Extension) cliStatusProcess(args []string) (string, error) {
	if len(args) < 1 {
		return formatCommandUsage("status", "<name|uuid>"), fmt.Errorf("process name or uuid required")
	}

	identifier := args[0]

	_, proc, err := e.resolveProcessIdentifier(identifier)
	if err != nil {
		return formatError(err.Error()), err
	}

	return formatProcessDetail(proc), nil
}

func (e *Extension) cliUnregisterProcess(args []string) (string, error) {
	if len(args) < 1 {
		return formatCommandUsage("unregister", "<name|uuid>"), fmt.Errorf("process name or uuid required")
	}

	identifier := args[0]

	processUUID, proc, err := e.resolveProcessIdentifier(identifier)
	if err != nil {
		return formatError(err.Error()), err
	}

	if proc.Status == processModels.ProcessStatusRunning {
		if err := e.host.StopApp(processUUID); err != nil {
			e.logger.Error("failed to stop process during unregister", "uuid", processUUID, "error", err)
			return formatError(fmt.Sprintf("Failed to stop running process: %s", err.Error())), err
		}
	}

	e.registryMutex.Lock()
	delete(e.processRegistry, processUUID)
	delete(e.nameToUUID, proc.Name)
	e.registryMutex.Unlock()

	if err := e.save(); err != nil {
		e.logger.Error("Failed to save process state after unregister", "error", err)
		return formatError(fmt.Sprintf("Process removed but failed to save: %s", err.Error())), err
	}

	return formatSuccess(fmt.Sprintf("Process unregistered: %s (%s)", proc.Name, processUUID)), nil
}
