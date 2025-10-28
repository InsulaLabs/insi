package svm

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/slp"
)

// simple virtual machine

/*


in the insi environment we have a set of operations that any entity can work with. These are

Cache
	set
	get
	delete
	iterate
Value
	set
	get
	delete
	iterate

Events
	Emit to topic
	Subscribe to topic

This defines the environment that the machine si working in. I will use this psuedo code to express the idea:




*/

type FunctionDefinition struct {
	Identifier string
	Body       slp.List
}

type Task struct {
	EveryDuration time.Duration
	Do            slp.List
}

type SVMProcessDefinition struct {
	operationDataScope string

	initBody          slp.List                // on init
	tasks             []Task                  // every
	interruptHandlers map[string]slp.Callable // interrupt-handler fns
}

type SVMProcessor struct {
	logger *slog.Logger

	processLogger *slog.Logger

	definition SVMProcessDefinition
	envData    fwi.KV     // cache or value - its irrelevant
	events     fwi.Events // event pub/sub

	running atomic.Bool
}

func NewProcessor(
	instanceScopeId string,
	logger *slog.Logger,
	kvBackend fwi.KV,
	eventsBackend fwi.Events,
) SVMProcessor {
	return SVMProcessor{
		logger:        logger,
		processLogger: logger.WithGroup("process"), // this is what "log" keyword logs to
		definition: SVMProcessDefinition{
			operationDataScope: instanceScopeId,
		},
		envData: kvBackend,
		events:  eventsBackend,
	}
}

func (p *SVMProcessor) LoadDefinition(instructions []byte) error {

	// TODO: Parse && store into processor definition for later interpreting
	return nil
}

func (p *SVMProcessor) Run(ctx context.Context) error {

	if p.running.Swap(true) {
		return errors.New("processor already running")
	}

	p.logger.Debug("AYYY")

	// TODO:
	// 1) Call on init, wait until complete success, then
	//		- this is where subscriptions will explicitly happen as seen above
	//		- if this fails, the processor will not start and the error will be returned
	// 2) Kick off all scheduled "every" list task

	return nil
}
