/*
	Each one of these is spawned from a "process" top level command (see example.svm)
*/

package svm

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/slp"
)

type FunctionDefinition struct {
	Identifier string
	Body       slp.List
}

type Task struct {
	EveryDuration time.Duration
	Do            slp.List
}

type SVMProcessDefinition struct {

	// first arg to "process" command
	name string

	// data-scope
	operationDataScope string

	// on-init
	initBody slp.List

	// on-kill
	killBody slp.List

	// every
	tasks []Task

	// static top-level functions on the process
	staticFns map[string]slp.Callable
}

type SVMProcess struct {
	logger *slog.Logger

	processLogger *slog.Logger

	definition SVMProcessDefinition
	envData    fwi.KV     // cache or value - its irrelevant
	events     fwi.Events // event pub/sub

	running atomic.Bool
}

type SVMProcessBuilder struct {
	logger        *slog.Logger
	kvBackend     fwi.KV
	eventsBackend fwi.Events

	name      string
	dataScope string
	initBody  slp.List
	killBody  slp.List
	tasks     []Task
	staticFns map[string]slp.Callable
}

func NewProcessBuilder() SVMProcessBuilder {
	return SVMProcessBuilder{}
}

func (b *SVMProcessBuilder) WithLogger(logger *slog.Logger) *SVMProcessBuilder {
	b.logger = logger
	return b
}

func (b *SVMProcessBuilder) WithKVBackend(kvBackend fwi.KV) *SVMProcessBuilder {
	b.kvBackend = kvBackend
	return b
}

func (b *SVMProcessBuilder) WithEventsBackend(eventsBackend fwi.Events) *SVMProcessBuilder {
	b.eventsBackend = eventsBackend
	return b
}

func (b *SVMProcessBuilder) WithName(name string) *SVMProcessBuilder {
	b.name = name
	return b
}

func (b *SVMProcessBuilder) WithDataScope(dataScope string) *SVMProcessBuilder {
	b.dataScope = dataScope
	return b
}

func (b *SVMProcessBuilder) WithInitBody(initBody slp.List) *SVMProcessBuilder {
	b.initBody = initBody
	return b
}

func (b *SVMProcessBuilder) WithKillBody(killBody slp.List) *SVMProcessBuilder {
	b.killBody = killBody
	return b
}

func (b *SVMProcessBuilder) WithTasks(tasks []Task) *SVMProcessBuilder {
	b.tasks = tasks
	return b
}

func (b *SVMProcessBuilder) WithStaticFns(staticFns map[string]slp.Callable) *SVMProcessBuilder {
	b.staticFns = staticFns
	return b
}

func (b *SVMProcessBuilder) Build() SVMProcess {
	return SVMProcess{
		logger:        b.logger,
		processLogger: b.logger.WithGroup("process"),
		definition: SVMProcessDefinition{
			name:               b.name,
			operationDataScope: b.dataScope,
			initBody:           b.initBody,
			killBody:           b.killBody,
			tasks:              b.tasks,
			staticFns:          b.staticFns,
		},
		envData: b.kvBackend,
		events:  b.eventsBackend,
	}
}

// ----------

// Called after build to ensure that the machine is in a state that can be excueted upon
func (x *SVMProcess) Validate() error {

	// TODO: check bodies, etc etc
	return nil
}

// ----------

func (x *SVMProcess) Run(ctx context.Context) error {

	// Start off the process
	return nil
}
