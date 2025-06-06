package chat

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	db_models "github.com/InsulaLabs/insi/db/models"
)

var ErrContextCancelled = errors.New("context cancelled")

type GenerativeInstance struct {
	logger         *slog.Logger
	ctx            context.Context
	td             *db_models.TokenData
	messageHistory []ChatMessage
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
				// Simulate work / delay between chunks
				time.Sleep(50 * time.Millisecond)
			}
		}
		return nil
	}

	// This is the main loop that will run until the context is cancelled or the output buffer is closed
	for {
		select {
		case <-gi.ctx.Done():
			return
		case data := <-gi.outputbuffer:
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

func (gi *GenerativeInstance) handleResponse() error {

	gi.bsend("Hello, world!")
	time.Sleep(1 * time.Second)
	gi.bsend("Hello, world!")
	time.Sleep(1 * time.Second)

	return nil
}

// ---------------------------------------- Response Generation ----------------------------------------
