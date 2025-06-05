package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/client"
	"github.com/InsulaLabs/insi/models"
	"github.com/InsulaLabs/insi/runtime"
	"github.com/google/uuid" // For generating unique IDs
)

/*

The server mounts plugins by name on "/" so if we have a plugin called "static"
then we can serve directly from "/static" automatically, and serve out the static
files from the dir handed to the plugin on creation.

ALlowing the server to server static files without modifying the internals
*/

type ChatPlugin struct {
	logger    *slog.Logger
	prif      runtime.PluginRuntimeIF
	startedAt time.Time
}

var _ runtime.Plugin = &ChatPlugin{}

func New(logger *slog.Logger) *ChatPlugin {
	return &ChatPlugin{
		logger: logger,
	}
}

func (p *ChatPlugin) GetName() string {
	return "chat"
}

func (p *ChatPlugin) Init(prif runtime.PluginRuntimeIF) *runtime.PluginImplError {
	p.prif = prif
	p.startedAt = time.Now()

	p.logger.Info("Chat plugin initialized.")
	return nil
}

func (p *ChatPlugin) GetRoutes() []runtime.PluginRoute {
	return []runtime.PluginRoute{
		{
			Path:    "completions",
			Handler: http.HandlerFunc(p.handleChatCompletions),
			Limit:   1000,
			Burst:   1000,
		},
	}
}

func (p *ChatPlugin) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	apiLogger := p.logger.WithGroup("openai_completions_api")

	if r.Method != http.MethodPost {
		apiLogger.Warn("Invalid HTTP method received for /completions", "method", r.Method)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	apiLogger.Info("Received POST request for /completions")

	authHeader := r.Header.Get("Authorization")
	token := ""
	if strings.HasPrefix(authHeader, "Bearer ") {
		token = strings.TrimPrefix(authHeader, "Bearer ")
	}
	apiLogger.Info("Authorization token", "token_present", token != "", "token_length", len(token))

	td, valid := p.prif.RT_ValidateAuthToken(r, false)
	if !valid {
		apiLogger.Error("Invalid authorization token")
		http.Error(w, "Invalid authorization token", http.StatusUnauthorized)
		return
	}

	canContinue, timeToWait := p.checkApiKeyRateLimit(&td)
	if !canContinue {
		// Add header info for time to wait, converting milliseconds to seconds (ceiling)
		w.Header().Set("Retry-After", strconv.Itoa((timeToWait+999)/1000))
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	var request ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		apiLogger.Error("Failed to decode request body", "error", err.Error())
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	apiLogger.Info("Request details",
		"model", request.Model,
		"stream", request.Stream,
		"message_count", len(request.Messages),
		"max_tokens", request.MaxTokens,
	)

	mockPromptTokens := 0
	if len(request.Messages) > 0 {
		for _, msg := range request.Messages {
			mockPromptTokens += len(msg.Content) // very rough estimate
		}
	}

	completionID := "chatcmpl-" + uuid.New().String()
	createdTimestamp := nowTimestamp()

	output := make(chan string, 10) // Buffered channel for response parts

	// Goroutine to run handleResponse and close the output channel
	go func() {
		defer close(output)
		err := p.handleResponse(r.Context(), &td, request.Messages, output)
		if err != nil {
			// Log error from handleResponse.
			// Further error propagation to HTTP response might be complex, especially for streaming.
			apiLogger.Error("Error from handleResponse", "error", err.Error())
		}
	}()

	if request.Stream {
		apiLogger.Info("Handling stream request")
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			apiLogger.Error("Streaming unsupported by the underlying http.ResponseWriter")
			http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
			return
		}

		// First chunk with role
		firstChunk := ChatCompletionStreamResponse{
			ID:      completionID,
			Object:  "chat.completion.chunk",
			Created: createdTimestamp,
			Model:   request.Model,
			Choices: []StreamChoice{
				{
					Index: 0,
					Delta: DeltaMessage{Role: "assistant"},
				},
			},
		}
		jsonData, err := json.Marshal(firstChunk)
		if err != nil {
			apiLogger.Error("Failed to marshal first stream chunk", "error", err)
			// Cannot send http.Error if headers are already sent.
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", jsonData)
		flusher.Flush()

		// Subsequent chunks with content from handleResponse
		chunkIndex := 0
		for contentPart := range output {
			chunk := ChatCompletionStreamResponse{
				ID:      completionID,
				Object:  "chat.completion.chunk",
				Created: createdTimestamp,
				Model:   request.Model,
				Choices: []StreamChoice{
					{
						Index: 0,
						Delta: DeltaMessage{Content: contentPart},
					},
				},
			}
			jsonData, err := json.Marshal(chunk)
			if err != nil {
				apiLogger.Error("Failed to marshal stream data chunk", "error", err, "chunk_index", chunkIndex)
				// Stop streaming if a chunk fails to marshal
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", jsonData)
			flusher.Flush()
			apiLogger.Debug("Sent stream chunk", "chunk_index", chunkIndex, "content_length", len(contentPart))
			chunkIndex++
		}

		// Final chunk with finish reason
		finishReason := "stop"
		finalChunk := ChatCompletionStreamResponse{
			ID:      completionID,
			Object:  "chat.completion.chunk",
			Created: createdTimestamp,
			Model:   request.Model,
			Choices: []StreamChoice{
				{
					Index:        0,
					Delta:        DeltaMessage{}, // Empty delta
					FinishReason: &finishReason,
				},
			},
		}
		jsonData, err = json.Marshal(finalChunk)
		if err != nil {
			apiLogger.Error("Failed to marshal final stream chunk", "error", err)
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", jsonData)
		flusher.Flush()

		// Send DONE message
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
		apiLogger.Info("Stream completed")

	} else {
		apiLogger.Info("Handling non-stream request")
		var responseBuilder strings.Builder
		var actualCompletionTokens int

		for contentPart := range output {
			responseBuilder.WriteString(contentPart)
			actualCompletionTokens += len(contentPart) // Count characters as a proxy for tokens
		}
		fullResponseContent := responseBuilder.String()

		response := ChatCompletionResponse{
			ID:      completionID,
			Object:  "chat.completion",
			Created: createdTimestamp,
			Model:   request.Model,
			Choices: []Choice{
				{
					Index: 0,
					Message: ResponseMessage{
						Role:    "assistant",
						Content: fullResponseContent,
					},
					FinishReason: "stop",
				},
			},
			Usage: Usage{
				PromptTokens:     mockPromptTokens,
				CompletionTokens: actualCompletionTokens,
				TotalTokens:      mockPromptTokens + actualCompletionTokens,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			apiLogger.Error("Failed to encode non-stream response", "error", err.Error())
			// If encoding fails, attempt to send an HTTP error.
			// This might not work if headers were already somehow sent, but it's the best effort.
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		}
		apiLogger.Info("Non-stream response sent")
	}
}

type RateLimit struct {
	RPM     int
	LastReq time.Time
}

func (p *ChatPlugin) getRateLimitKey(td *models.TokenData) string {
	return fmt.Sprintf("chat:completions:limit:%s", td.UUID)
}

// return if can continue, and then time to wait if failure 0 means no wait for retry
func (p *ChatPlugin) checkApiKeyRateLimit(td *models.TokenData) (bool, int) {

	// Note: Witht he RT_Set and RT_Get cache calls there is the real possibility of a race condition
	// where the cache is set and the request is processed before the cache is updated.
	// This is a very low probability and the impact is minimal.
	// This is because we are syncing it across the distributed cluster. Since we only ever monotonically
	// increase the time stamp this is not a problem.
	// It may lead to one request being allowed that should not be, but this is a very low probability
	// and the impact is so minimal that it is not worth the complexity of a lock. - bosley
	key := p.getRateLimitKey(td)
	val, err := p.prif.RT_GetCache(key)
	if err != nil {
		if err == client.ErrKeyNotFound {
			defaultRL := RateLimit{
				RPM:     600, // 10 requests per second
				LastReq: time.Now(),
			}
			jsonBytes, err := json.Marshal(defaultRL)
			if err != nil {
				p.logger.Error("[INTERNAL ERROR] Error marshalling default rate limit", "error", err, "key", key)
				return false, 0 // Cannot proceed, 0 wait time (ambiguous for error)
			}
			// Set cache and allow this first request
			p.prif.RT_SetCache(key, string(jsonBytes), 1*time.Minute)
			p.logger.Info("Rate limit key not found, created default", "key", key, "rpm_limit", defaultRL.RPM)
			return true, 0 // Allowed, 0 wait time signifies no wait needed
		}
		// Other GetCache error
		p.logger.Error("[INTERNAL ERROR] Error getting rate limit from cache", "error", err, "key", key)
		return false, 0 // Cannot proceed, 0 wait time (ambiguous for error)
	}

	var rateLimit RateLimit
	err = json.Unmarshal([]byte(val), &rateLimit)
	if err != nil {
		p.logger.Error("[INTERNAL ERROR] Error unmarshalling rate limit from cache", "error", err, "key", key, "value", val)
		return false, 0 // Cannot proceed, 0 wait time (ambiguous for error)
	}

	if rateLimit.RPM <= 0 {
		p.logger.Error("Invalid RPM configuration in cache: must be positive", "rpm_value", rateLimit.RPM, "key", key)
		// Cannot proceed due to misconfiguration. 0 wait time is ambiguous.
		return false, 0
	}

	// Calculate the expected minimum interval between requests based on RPM.
	// e.g., if RPS (interpreted as RPM) is 20, interval is 1 minute / 20 = 3 seconds.
	expectedInterval := time.Minute / time.Duration(rateLimit.RPM)
	elapsedTime := time.Since(rateLimit.LastReq)

	if elapsedTime < expectedInterval {
		remainingWaitTime := expectedInterval - elapsedTime
		p.logger.Debug("Rate limit check failed: request too soon",
			"key", key,
			"elapsed_ms", elapsedTime.Milliseconds(),
			"expected_interval_ms", expectedInterval.Milliseconds(),
			"wait_for_ms", remainingWaitTime.Milliseconds(),
			"rpm_limit", rateLimit.RPM,
		)
		// Request is too soon. Return false (cannot proceed) and the remaining time to wait in milliseconds.
		return false, int(remainingWaitTime.Milliseconds())
	}

	// If we reach here, the request is allowed.
	// Update LastReq for the current successful request and save it back.
	rateLimit.LastReq = time.Now()
	jsonData, err := json.Marshal(rateLimit)
	if err != nil {
		p.logger.Error("[INTERNAL ERROR] Error marshalling updated rate limit for cache", "error", err, "key", key)
		return false, 0
	}

	if err := p.prif.RT_SetCache(key, string(jsonData), 1*time.Minute); err != nil {
		p.logger.Error("[INTERNAL ERROR] Error setting rate limit for cache", "error", err, "key", key)
		return false, 0
	}

	p.logger.Debug("Rate limit check passed", "key", key, "rpm_limit", rateLimit.RPM)
	// Request allowed. Return true. The second parameter 0 means "no wait needed".
	return true, 0
}

func (p *ChatPlugin) handleResponse(ctx context.Context, td *models.TokenData, messages []ChatMessage, output chan<- string) error {
	responseLogger := p.logger.WithGroup("response_handler")
	responseLogger.Info("Starting response generation", "num_messages", len(messages))

	if len(messages) == 0 {
		select {
		case output <- "I am a helpful assistant. How can I help you today?": // Default response if no input
			responseLogger.Debug("Sent default 'no messages' response to output channel")
		case <-ctx.Done():
			responseLogger.Warn("Context cancelled before sending default 'no messages' response", "error", ctx.Err())
			return ctx.Err()
		}
		return nil
	}

	// For this example, we'll echo the last message content.
	// In a real application, this is where LLM interaction would happen.
	lastMessageContent := messages[len(messages)-1].Content
	fullResponse := "Echo: " + lastMessageContent

	// Simulate streaming by sending the response in chunks (e.g., word by word)
	words := strings.Fields(fullResponse)
	if len(words) == 0 && len(fullResponse) > 0 { // Handle case where fullResponse is not empty but has no spaces (e.g. "Echo:Test")
		words = []string{fullResponse}
	}

	for i, word := range words {
		chunk := word
		// Add a space after each word, except for the last one, only if there are more words.
		// This prevents adding a trailing space if the response is a single word or the last word.
		if i < len(words)-1 {
			chunk += " "
		}

		select {
		case <-ctx.Done():
			responseLogger.Warn("Context cancelled during response generation", "error", ctx.Err())
			return ctx.Err()
		case output <- chunk:
			responseLogger.Debug("Sent chunk to output channel", "chunk", chunk)
			// Simulate work / delay between chunks
			time.Sleep(50 * time.Millisecond)
		}
	}

	responseLogger.Info("Finished sending all response content to output channel.")
	return nil
}

/*

Notes:


Since we are potentially execing on an env we could make them bunmp ttld with identifiers
being the time the first message was sent, the hash of the first message + the uuid of the user
HASHED as a key in the cache. We can then map that to a "chat session" which is a vm instance
that will be used to execute things

"Branching" Could be just appending the branch point




*/
