package vm

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/InsulaLabs/insi/plugins"
)

type Locator struct {
	BlockNumber uint32
	BlockLine   uint32
	Instruction uint32
}

type DataReceiver interface {

	// When the VM detects a "load object asset" it will rettrieve the UUID in-context
	// to the conversation and issue a call to this so that the caller can actually
	// load the asset into whatever they are working with
	// ARG: bnum: block number (which submitted block caused it)
	// ARG: bloc: block line location (where in the block it came from)
	// ARG: insNum: instruction number (which instruction in the block caused it)
	// ARG: objectUUID: uuid of the object to load
	// (this is distilled from the island's resource map)
	OnObjectAsset(locator Locator, objectUUID string)

	// Blooming is taking the users concept map and taking "lesser words" to link
	// an established "concept" that they have already created to link conceptually
	// to wherever they asked for it
	// ARG: locator: where in the block it came from
	// ARG: from: the concept that the user has already created
	// ARG: to: the concept that the user wants to link to
	OnBloomConcept(locator Locator, from string, to []string)

	// ARG: locator: where in the block it came from
	// ARG: toolUUID: uuid of the tool that the user wants to link to
	// ARG: toolName: name of the tool that the user wants to link to
	OnLinkTool(locator Locator, toolName string)
}

type VMError struct {
	Message string
	Code    int
}

func (e *VMError) Error() string {
	return fmt.Sprintf("VM error: %s (code: %d)", e.Message, e.Code)
}

var (
	ErrVMNotInitialized     = &VMError{Message: "VM not initialized", Code: 1}
	ErrVMAlreadyInitialized = &VMError{Message: "VM already initialized", Code: 2}
	ErrVMInvalidMode        = &VMError{Message: "Invalid VM mode", Code: 3}

	ErrVMNotImplemented = &VMError{Message: "VM not implemented", Code: 32000}
)

type Config struct {
	VMInstanceUUID string // unique id for the vm instance
	Logger         *slog.Logger
	TargetedIsland *plugins.Island
	AvailableTools []string // list of AI mountable tools that the user can invoke (e.g. "sqlkit", "imgkit", "wekit")
}

type syncs struct {
	modeMu  sync.RWMutex
	toolsMu sync.RWMutex
}

type VM struct {
	mode           VMMode
	vmInstanceUUID string // unique id for the vm instance
	logger         *slog.Logger

	syncs syncs

	spc *SPC
	cag *CAG

	availableTools map[string]bool

	targetedIsland *plugins.Island
}

type VMMode string

const (
	VMModeNone                 VMMode = "none"
	VMModeSimplePromptCrafting VMMode = "simple-prompt-crafting"
	VMModeCoordinatedAgentic   VMMode = "coordinated-agentic"
)

func New(config Config) *VM {

	availableTools := make(map[string]bool)
	for _, tool := range config.AvailableTools {
		availableTools[tool] = true
	}

	v := &VM{
		mode:           VMModeNone,
		vmInstanceUUID: config.VMInstanceUUID,
		logger:         config.Logger,
		availableTools: availableTools,
		targetedIsland: config.TargetedIsland,
	}

	v.spc = NewSPC(v)
	v.cag = NewCAG(v)
	return v
}

func (v *VM) GetVMInstanceUUID() string {
	return v.vmInstanceUUID
}

func (v *VM) IsToolAvailable(toolName string) bool {
	v.syncs.toolsMu.RLock()
	defer v.syncs.toolsMu.RUnlock()
	_, ok := v.availableTools[toolName]
	return ok
}

func (v *VM) InitializeMode(mode VMMode) error {
	v.syncs.modeMu.Lock()
	defer v.syncs.modeMu.Unlock()
	if v.mode != VMModeNone {
		return ErrVMAlreadyInitialized
	}

	switch mode {
	case VMModeNone:
		return ErrVMInvalidMode
	case VMModeSimplePromptCrafting:
		v.mode = VMModeSimplePromptCrafting
	case VMModeCoordinatedAgentic:
		v.mode = VMModeCoordinatedAgentic
	default:
		return ErrVMInvalidMode
	}
	return nil
}

func (v *VM) ready() bool {
	v.syncs.modeMu.RLock()
	defer v.syncs.modeMu.RUnlock()
	return v.mode != VMModeNone
}

// Submit a block of code to analyze and work with. changes internal state.
// ready for next step OR Finalize() which will return the final result.
func (v *VM) Step(block string) error {
	if !v.ready() {
		return ErrVMNotInitialized
	}
	v.logger.Info("VM: Step", "block", len(block))
	switch v.mode {
	case VMModeSimplePromptCrafting:
		return v.spc.Step(block)
	case VMModeCoordinatedAgentic:
		return v.cag.Step(block)
	default:
		return ErrVMInvalidMode
	}
}

// Finalize the VM. returns the final result.
func (v *VM) Finalize() error {
	if !v.ready() {
		return ErrVMNotInitialized
	}
	v.logger.Info("VM: Finalize")
	switch v.mode {
	case VMModeSimplePromptCrafting:
		return v.spc.Finalize()
	case VMModeCoordinatedAgentic:
		return v.cag.Finalize()
	}
	return nil
}
