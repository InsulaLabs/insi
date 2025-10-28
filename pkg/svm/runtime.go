package svm

import (
	"context"
	"errors"

	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/slp"
)

var (
	ErrRuntimeNotIdle       = errors.New("runtime not idle")
	ErrRuntimeNotInitalized = errors.New("runtime not initalized")
)

type RuntimeState int

const (
	RuntimeStateIdle RuntimeState = iota
	RuntimeStateInitalized
	RuntimeStateTasksRunning
	RuntimeStateStopped
	RuntimeStateError
)

type Runtime struct {
	rtCtx context.Context
	state RuntimeState

	kv     fwi.KV
	events fwi.Events

	/*
		Startup shutdown, and process list as to be build by the runtime builder
		when loading in data
	*/
	startupBody  slp.List
	shutdownBody slp.List
	processes    map[string]SVMProcess
}

var _ slp.Env = &Runtime{}

func (r *Runtime) Context() context.Context {
	return r.rtCtx
}

func (r *Runtime) ResolveIdentifier(ctx context.Context, name string) slp.Obj {
	return slp.Obj{Type: slp.OBJ_TYPE_NONE, D: slp.None{}}
}

var _ slp.Backend = &Runtime{}

func (r *Runtime) ExecuteList(env slp.Env, list slp.List) slp.Obj {
	return slp.Obj{Type: slp.OBJ_TYPE_NONE, D: slp.None{}}
}

func (r *Runtime) ResolveCallable(env slp.Env, name string) slp.Obj {
	return slp.Obj{Type: slp.OBJ_TYPE_NONE, D: slp.None{}}
}

/*

	ALl commands will have to have an implementation or handler in the runtime as the runtime
	is what actually orchestrates everything. Once a runtime si built from source we startup
	(the function) and start off all the "every" functions and start supporting the subscriptions
	etc.

	"set <identifier> <resolved-value>"
	"get <identifier>" => Obj

	do
	etc
*/

func (r *Runtime) GetState() RuntimeState {
	return r.state
}

func (r *Runtime) OnInit() error {
	if r.state != RuntimeStateIdle {
		return ErrRuntimeNotIdle
	}

	r.state = RuntimeStateInitalized

	// TODO: use self (r.ExecuteList) on the procedssor oin init kist that was parsed out

	return nil
}

func (r *Runtime) BeginScheduledTasks() error {
	if r.state != RuntimeStateInitalized {
		return ErrRuntimeNotInitalized
	}

	r.state = RuntimeStateTasksRunning

	// TODO: Begin scheduled tasks, each one will use self (r.ExecuteList) to execute the body

	return nil
}
