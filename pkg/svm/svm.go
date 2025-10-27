package svm

import (
	"context"
	"log/slog"
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


//------------------------------------------------------------

(data-scope "some-arbitrary-data-to-scope-operations-to")


(interrupt-handler "topic-name-A" data (do
	(log "Received event on topic 'topic-name-A, with data:'" data)
))

(interrupt-handler "topic-name-B" data (do
	(log "Received event on topic 'topic-name-B, with data:'" data)
))

(every "1s" (do
	(emit "topic-name-A" "Hello, world!")
))

(every "5s" (do
	(emit "topic-name-B", "AYYYYYYY")
))


(every "1s" (do

	; Each "Every" is basically a "main" tick function. All you do is define one of these at the top level
	; and its body will run each tick

))

(on-init (do
	(init "topic-name-A")
	(init "topic-name-B")
))



Top level syntax will differ from "inner" syntax to ensure that "every" and "interrupt-handler" are not available unless
we are parsing the top level scope.

We will essentially be doing a bisection or 2-pass filter on these files to seperate out the setup functions:
	interrupt-handler "SOME NAME" <data> <body>

	every <duration> <body>

	on-init <body>

These top 3 have some rules as well
	"every" function if given the same duration but defined in separate blocks will not be guaranteed to be executed synchronously.
		- I will not pre-merge same-duration bodies in any order as that will inevitably lead to undue complexity for little benefit

	interrupt-handler
		- This is a body similar to "every" but is executed whenever an event is received by the processor.
			This definition has a special <data> argument that will be captured by the body of the statement so "data" should
			be treated by the programmer as a "keyword" as it will be present in every root parse context unabashedly.

	on-init
		- Similiar to a microcontroller's setup function, this can be used to define actions that run once at the very beginning
			of vm execution.

	data-scope
		- sets the data scope for the fwi KVS


Available commands inside any "do" list:

(data-scope "test")
(every "1s" (do

	(try
		(snx x 0)
		(log "x already existed (okay)"))

	(log (get x))





))


The values when stopred into the fwi interface will have to be encoded into the parser's representation of the data so that when they are "gotten" via "get"
they can be resoled into a workable object


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

	initBody          slp.List
	tasks             []Task
	interruptHandlers map[string]slp.List
}

type SVMProcessor struct {
	logger     *slog.Logger
	definition SVMProcessDefinition
	envData    fwi.KV     // cache or value - its irrelevant
	events     fwi.Events // event pub/sub

}

func NewProcessor(
	instanceScopeId string,
	logger *slog.Logger,
	kvBackend fwi.KV,
	eventsBackend fwi.Events,
) SVMProcessor {
	return SVMProcessor{
		logger: logger,
		definition: SVMProcessDefinition{
			operationDataScope: instanceScopeId,
		},
		envData: kvBackend,
		events:  eventsBackend,
	}
}

func (p *SVMProcessor) Run(ctx context.Context) error {

	p.logger.Debug("AYYY")

	return nil
}
