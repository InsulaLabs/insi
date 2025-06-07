package chat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/InsulaLabs/insi/ai/kit"
	db_models "github.com/InsulaLabs/insi/db/models"
	"github.com/InsulaLabs/insi/runtime"
	"github.com/InsulaLabs/insi/service/chat/insikit"
	"github.com/InsulaLabs/insi/service/models"
	service_models "github.com/InsulaLabs/insi/service/models"
	"github.com/InsulaLabs/insi/slp"
	"github.com/tmc/langchaingo/llms"
)

var ErrContextCancelled = errors.New("context cancelled")

type GenerativeInstance struct {
	prif         runtime.ServiceRuntimeIF
	logger       *slog.Logger
	ctx          context.Context
	td           *db_models.TokenData
	insiApiKey   string
	ccr          ChatCompletionRequest
	provider     service_models.Provider
	output       chan<- string
	outputbuffer chan string

	err   error
	errMu sync.Mutex
	wg    sync.WaitGroup

	sending atomic.Bool

	tokensPermitted int
}

/*
Buffer something too send and run away. This defers (logically) the possibility of an error
at specific call points to simplify the code and logic. We use bsend to send AI AND log/system
messages through and the handleSendRoutine will flatten them out to make a continuous stream of
readable text.
*/
func (gi *GenerativeInstance) bsend(data string) {
	gi.outputbuffer <- data
}

/*
Throughout the process of figuring out what the user wants in the short time we have to make
the response, this function is what pumps out "log" or "state" messages from the system to the user
in a way that is smooth and readable
*/
func (gi *GenerativeInstance) handleSendRoutine() {
	gi.wg.Add(1)
	defer gi.wg.Done()

	defer func() {
		//recover from any panics
		if r := recover(); r != nil {
			gi.logger.Error("panic in handleSendRoutine", "error", r)
		}
	}()

	sendData := func(data string) error {
		gi.sending.Store(true)
		defer gi.sending.Store(false)

		select {
		case <-gi.ctx.Done():
			return ErrContextCancelled
		case gi.output <- data:
			gi.logger.Debug("Sent chunk to output channel", "chunk", data)
		}
		return nil
	}

	// This is the main loop that will run until the context is cancelled or the output buffer is closed
	for {
		select {
		case <-gi.ctx.Done():
			return
		case data, ok := <-gi.outputbuffer:
			if !ok {
				return
			}
			if err := sendData(data); err != nil {
				gi.errMu.Lock()
				gi.err = err
				gi.errMu.Unlock()
				return
			}
		}
	}
}

// ---------------------------------------- Response Generation ----------------------------------------

func (gi *GenerativeInstance) handleResponse() ([]llms.MessageContent, error) {
	defer close(gi.outputbuffer)
	/*

		Load the users targetd island based on the model slug provided via the ccr

	*/

	slug := gi.ccr.Model

	insiClient, err := gi.prif.RT_GetClientForToken(gi.insiApiKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create insi client for kvkit: %w", err)
	}

	islands, err := insiClient.IterateIslands(0, 100)
	if err != nil {
		return nil, fmt.Errorf("failed to iterate islands: %w", err)
	}

	var targetIsland *models.Island
	for _, island := range islands {
		if island.ModelSlug == slug {
			targetIsland = &models.Island{
				Entity:      island.Entity,
				EntityUUID:  island.EntityUUID,
				UUID:        island.UUID,
				Name:        island.Name,
				ModelSlug:   island.ModelSlug,
				Description: island.Description,
			}
			break
		}
	}

	if targetIsland == nil {
		return nil, fmt.Errorf("island not found")
	}

	var llm llms.Model
	switch gi.provider.Provider {
	case service_models.SupportedProviderOpenAI:
		llm, err = kit.GetOpenAILLM(gi.provider.APIKey)
		if err != nil {
			return nil, err
		}
	case service_models.SupportedProviderAnthropic:
		llm, err = kit.GetAnthropicLLM(gi.provider.APIKey)
		if err != nil {
			return nil, err
		}
	case service_models.SupportedProviderXAI:
		llm, err = kit.GetGrokLLM(gi.provider.APIKey)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported provider: %s", gi.provider.Provider)
	}

	k := kit.NewKit(gi.ctx, llm, gi.logger)

	messageHistory := make([]llms.MessageContent, len(gi.ccr.Messages))
	for i, message := range gi.ccr.Messages {
		var llmRole llms.ChatMessageType

		switch message.Role {
		case "user":
			llmRole = llms.ChatMessageTypeHuman
		case "assistant":
			llmRole = llms.ChatMessageTypeAI
		default:
			// Default to attempt direct mapping
			llmRole = llms.ChatMessageType(message.Role)
		}

		messageHistory[i] = llms.MessageContent{
			Role: llmRole,
			Parts: []llms.ContentPart{
				llms.TextContent{
					Text: message.Content,
				},
			},
		}
	}

	// Check the last user message for macros and load tools if necessary.
	lastUserMessage := ""
	lastUserMessageIndex := -1 // To store the index of the last user message
	for i := len(messageHistory) - 1; i >= 0; i-- {
		if messageHistory[i].Role == llms.ChatMessageTypeHuman {
			if text, ok := messageHistory[i].Parts[0].(llms.TextContent); ok {
				lastUserMessage = text.Text
				lastUserMessageIndex = i
				break
			}
		}
	}

	if lastUserMessage != "" {
		tools, scrubbedMessage, err := gi.parseMacro(lastUserMessage)
		if err != nil {
			gi.logger.Warn("failed to parse or execute macro", "error", err)
			gi.bsend(fmt.Sprintf("[System: Error processing command: %v]\n", err))
		} else {
			if scrubbedMessage != lastUserMessage && lastUserMessageIndex != -1 {
				// The message was changed, so update it in the history
				if text, ok := messageHistory[lastUserMessageIndex].Parts[0].(llms.TextContent); ok {
					text.Text = scrubbedMessage
					messageHistory[lastUserMessageIndex].Parts[0] = text
				}
			}

			if len(tools) > 0 {
				gi.bsend(fmt.Sprintf("[System: Loaded %d tool(s)]\n", len(tools)))
				for _, t := range tools {
					k.WithTool(t)
				}
			}
		}
	}

	// Respect the user's token limit
	k.WithMaxTokens(gi.tokensPermitted)

	response := k.Complete(gi.ctx, messageHistory, kit.WithStreamingFunc(func(ctx context.Context, chunk []byte) error {
		gi.bsend(string(chunk))
		return nil
	}))

	return response, nil
}

// ---------------------------------------- Response Generation ----------------------------------------

func (gi *GenerativeInstance) parseMacro(message string) ([]kit.Tool, string, error) {

	program, remainingMessage, err := slp.ParseBlock(message)
	if err != nil {
		return nil, message, err
	}

	if program == nil {
		return nil, message, nil
	}

	/*

		(\'COMMAND ARG ARG ARG ARG)

		Commands:

			cc  - compact context
				Tool does not yet exist:
					It will take the full length of the messages, have a forge and crit "compact" the context
					to ensure all essential information is preserved (current message not touched) this will
					ensure the essential history of the conversation is preserved while chatting

			help - help
				Tool does not yet exist:
					It will NOT send to the LLM, instead, it will return a fake llm response and stream out
					the moment it is called (directly to output buffer)

			tl - Tool load


				(\'tl tool-one tool-two)



	*/

	tools := make([]kit.Tool, 0)

	commandMap := map[string]slp.Function{
		"cc": func(p *slp.Proc, args []slp.Value) (slp.Value, error) {
			gi.clearContext()
			return slp.Value{
				Type:  slp.DataTypeString,
				Value: "Context cleared",
			}, nil
		},
		"help": func(p *slp.Proc, args []slp.Value) (slp.Value, error) {
			gi.help()
			return slp.Value{
				Type:  slp.DataTypeString,
				Value: "Help message sent to output buffer",
			}, nil
		},
		"tl": func(p *slp.Proc, args []slp.Value) (slp.Value, error) {
			loadedTools, err := gi.loadTools(args)
			message := ""
			if err != nil {
				message = "Error loading tools"
			} else {
				message = "Tools loaded successfully"
			}
			tools = append(tools, loadedTools...)
			return slp.Value{
				Type:  slp.DataTypeString,
				Value: message,
			}, err
		},
	}

	// Create a new environment for the SLP processor
	env := slp.NewEnv(nil)
	for cmdName, command := range commandMap {
		env.Set(cmdName, slp.Value{
			Type:  slp.DataTypeFunction,
			Value: command,
		})
	}

	proc := slp.NewProcessor(env, program)

	return tools, remainingMessage, proc.Run()
}

func (gi *GenerativeInstance) clearContext() {

	fmt.Println("clearContext")
}

func (gi *GenerativeInstance) help() {

	helpText := `

	This is some help text!!!!!

	`

	gi.bsend(helpText)
}

func (gi *GenerativeInstance) loadTools(args []slp.Value) ([]kit.Tool, error) {
	gi.logger.Debug("loading tools from macro", "args", args)

	tools := make([]kit.Tool, 0)
	for _, arg := range args {
		loadedTools, err := gi.loadTool(arg)
		if err != nil {
			return nil, err
		}
		tools = append(tools, loadedTools...)
	}

	return tools, nil
}

func (gi *GenerativeInstance) loadTool(arg slp.Value) ([]kit.Tool, error) {
	var toolName string
	switch arg.Type {
	case slp.DataTypeIdentifier:
		val, ok := arg.Value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected type for identifier value: %T", arg.Value)
		}
		toolName = val
	default:
		return nil, fmt.Errorf("loadTool expects an identifier, got %s", arg.Type)
	}

	gi.logger.Info("loading tool kit", "name", toolName)

	switch toolName {
	case "kvs":
		insiClient, err := gi.prif.RT_GetClientForToken(gi.insiApiKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create insi client for kvkit: %w", err)
		}

		kvk := insikit.NewKVKit(insiClient)
		return kvk.GetAllTools(), nil
	default:
		return nil, fmt.Errorf("unknown tool kit: %s", toolName)
	}
}
