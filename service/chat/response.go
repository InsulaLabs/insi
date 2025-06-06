package chat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/InsulaLabs/insi/ai/kit"
	db_models "github.com/InsulaLabs/insi/db/models"
	service_models "github.com/InsulaLabs/insi/service/models"
	"github.com/tmc/langchaingo/llms"
)

var ErrContextCancelled = errors.New("context cancelled")

type GenerativeInstance struct {
	logger         *slog.Logger
	ctx            context.Context
	td             *db_models.TokenData
	messageHistory []ChatMessage
	provider       service_models.Provider
	output         chan<- string
	outputbuffer   chan string

	err   error
	errMu sync.Mutex
	wg    sync.WaitGroup

	sending atomic.Bool
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

	sendData := func(data string) error {
		words := strings.Fields(data)
		if len(words) == 0 && len(data) > 0 {
			words = []string{data}
		}

		gi.sending.Store(true)
		defer gi.sending.Store(false)

		for i, word := range words {
			chunk := word
			// Add a space after each word, except for the last one, only if there are more words.
			// This prevents adding a trailing space if the response is a single word or the last word.
			if i < len(words)-1 {
				chunk += " "
			}

			select {
			case <-gi.ctx.Done():
				// Error in here because we were working, but its ultimately okay to ignore
				return ErrContextCancelled
			case gi.output <- chunk:
				gi.logger.Debug("Sent chunk to output channel", "chunk", chunk)

			}
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

	var llm llms.Model
	var err error
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

	messageHistory := make([]llms.MessageContent, len(gi.messageHistory))
	for i, message := range gi.messageHistory {
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

	response := k.Complete(gi.ctx, messageHistory, kit.WithStreamingFunc(func(ctx context.Context, chunk []byte) error {
		gi.bsend(string(chunk))
		return nil
	}))

	return response, nil
}

// ---------------------------------------- Response Generation ----------------------------------------
